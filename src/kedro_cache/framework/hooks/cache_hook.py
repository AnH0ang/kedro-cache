import functools
import logging
import os
from logging import Logger
from typing import Any, Callable, Dict, Optional

from kedro.config import MissingConfigException
from kedro.framework.context import KedroContext
from kedro.framework.hooks import hook_impl
from kedro.io import DataCatalog, MemoryDataSet
from kedro.pipeline.node import Node
from sqlitedict import SqliteDict

from kedro_cache.config.models import KedroCacheConfig
from kedro_cache.framework.hooks.cache import NodeCache
from kedro_cache.framework.hooks.hash import hash_datadict, hash_function_body

# import shelve
CACHE_CONFIG_KEY = "__kedro_cache_config__"
RERUN_ENV_VAR = "KEDRO_CACHE_RERUN"


class KedroCacheHook:
    """The hook that is used to enable caching.

    The hook uses the cached output of a node if all four conditions are met:

    1. The **cache of the input** matches the previous run.
    2. The **function body** of the node has not changed.
    3. The outputs of the node exist in the **catalog**.
    4. The output **cache** of the node matches the previous run.

    All check are run during the `before_node_run` hook. If all conditions are
    met, the node function is overwritten with a function that returns the cached
    data. Else, the node function is left unchanged. After the node has run, during
    the `after_node_run` hook, the output cache is updated and stored in the
    a `shelve` database.
    """

    node2original_function: Dict[str, Callable]

    def __init__(self) -> None:
        self.node2original_function = {}

    @hook_impl
    def after_context_created(
        self,
        context: KedroContext,
    ) -> None:
        """Hooks to be invoked after a `KedroContext` is created.

        This hook reads the `kedro-cache` configuration from the `cache.yml` from the
        `conf` folder of the Kedro project and stores it in the `cache_config``
        attribute of the hook.

        Args:
            context: The newly created context.
        """
        # Find the KedroCacheConfig in the context
        try:
            conf_cache_yml = context.config_loader.get("cache*", "cache*/**")
        except MissingConfigException:
            self._logger.warning("No 'cache.yml' config file found in environment")
            conf_cache_yml = {}
        cache_config = KedroCacheConfig.parse_obj(conf_cache_yml)

        # store in context for interactive use
        context.__setattr__("cache_config", cache_config)

        # store in catalog for further reuse
        context.catalog._data_sets[CACHE_CONFIG_KEY] = MemoryDataSet(cache_config)

    @hook_impl
    def before_node_run(
        self,
        node: Node,
        catalog: DataCatalog,
        inputs: Dict[str, Any],
        is_async: bool,
        session_id: str,
    ) -> None:
        """Hook to be invoked before a node runs.

        Args:
            node: The `Node` to run.
            catalog: A `DataCatalog` containing the node's inputs and outputs.
            inputs: The dictionary of inputs dataset.
                The keys are dataset names and the values are the actual loaded input
                data, not the dataset instance.
            is_async: Whether the node was run in `async` mode.
            session_id: The id of the session.
        """
        # load cache config from catalog
        assert catalog.exists(CACHE_CONFIG_KEY), "Cache config must exist"
        cache_config = catalog.load(CACHE_CONFIG_KEY)

        # get the node cache
        with SqliteDict(str(cache_config.cachedb_dir)) as cachedb:
            cache: Optional[NodeCache] = cachedb.get(node.name, None)

        # 1. check if caching is enabled
        caching_disabled = self._caching_disabled(node, cache_config)
        if caching_disabled:
            return

        # 2. if cache does not exist, exit
        if cache is None:
            return

        # sanity check: The cache name should match the node name
        assert cache.node_name == node.name, "Node name must match"

        # 3. if input hash does not match, exit
        input_hash = hash_datadict(inputs)
        if input_hash != cache.input_hash:
            return

        # 4. if function body has changed, exit
        function_hash = hash_function_body(node.func)
        if function_hash != cache.function_hash:
            return

        # 5. if all output does not exist in catalog, exit
        all_outputs_exits = all(catalog.exists(name) for name in node.outputs)
        if not all_outputs_exits:
            return

        # 6. if output hash does not match, exit
        output_dict = {name: catalog.load(name) for name in node.outputs}
        output_hash = hash_datadict(output_dict)
        if output_hash != cache.output_hash:
            return

        # if all conditions are met, overwrite the node function
        self.node2original_function[node.name] = node.func
        if len(node.outputs) == 1:
            output_value_or_values = output_dict[node.outputs[0]]
        else:
            output_value_or_values = tuple(output_dict[name] for name in node.outputs)

        # node needs to be wrapped to keep the original signature
        node.func = functools.wraps(node.func)(lambda *a, **ka: output_value_or_values)

        self._logger.info(f"Using cached output for node: {node.name}")

    @hook_impl
    def after_node_run(
        self,
        node: Node,
        catalog: DataCatalog,
        inputs: Dict[str, Any],
        outputs: Dict[str, Any],
        is_async: bool,
        session_id: str,
    ) -> None:
        """Hook to be invoked after a node runs.

        Args:
            node: The ``Node`` that ran.
            catalog: A ``DataCatalog`` containing the node's inputs and outputs.
            inputs: The dictionary of inputs dataset.
                The keys are dataset names and the values are the actual loaded input
                data, not the dataset instance.
            outputs: The dictionary of outputs dataset.
                The keys are dataset names and the values are the actual computed output
                data, not the dataset instance.
            is_async: Whether the node was run in ``async`` mode.
            session_id: The id of the session.
        """
        # load cache config from catalog
        assert catalog.exists(CACHE_CONFIG_KEY), "Cache config must exist"
        cache_config = catalog.load(CACHE_CONFIG_KEY)

        # restore the original function if it was overwritten
        node.func = self.node2original_function.pop(node.name, node.func)

        # calculate hashes
        input_hash = hash_datadict(inputs)
        output_hash = hash_datadict(outputs)
        function_hash = hash_function_body(node.func)

        # write cache to db
        with SqliteDict(str(cache_config.cachedb_dir)) as cachedb:
            cachedb[node.name] = NodeCache(
                node_name=node.name,
                input_hash=input_hash,
                output_hash=output_hash,
                function_hash=function_hash,
            )
            cachedb.commit()

    @property
    def _logger(self) -> Logger:
        # prepend the name of the plugin with "kedro" to use its logger
        return logging.getLogger("kedro" + "." + __name__)

    def _caching_disabled(self, node: Node, cache_config: KedroCacheConfig) -> bool:
        """Check if caching is disabled for a given node.

        Args:
            node: The node to check.

        Returns:
            True if caching is disabled, False otherwise.
        """
        short_name = node.name.split("(")[0]
        node_disabled = short_name in cache_config.ignore_nodes

        all_disabled = cache_config.rerun_all
        env_disabled = os.environ.get(RERUN_ENV_VAR, "0") == "1"
        return node_disabled or all_disabled or env_disabled


cache_hook = KedroCacheHook()
