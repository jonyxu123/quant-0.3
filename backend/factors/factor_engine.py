"""因子计算主引擎"""

import duckdb

import pandas as pd

import numpy as np

from loguru import logger

from concurrent.futures import ThreadPoolExecutor, as_completed

from config import DB_PATH, DIMENSION_PREFIXES

class FactorEngine:

    """因子计算主引擎

    通过注册表统一调度27大维度208+因子的计算，每个维度模块导出 register() + compute() 两个函数
    """

    

    def __init__(self, db_path=DB_PATH):

        self.db_path = db_path

        self.conn = duckdb.connect(db_path)

        self._registry = self._build_factor_registry()

    

    def _build_factor_registry(self) -> dict:

        """构建因子注册表 {factor_id: {dimension, name, direction, compute_func}}"""

        registry = {}

        

        from backend.factors import (

            value_factors, profitability_factors, growth_factors,

            quality_factors, momentum_factors, liquidity_factors,

            analyst_factors, institution_factors, chip_factors,

            tech_pattern_factors, event_factors, money_flow_factors,

            volatility_factors, sentiment_factors, calendar_factors,

            governance_factors, analyst_rating_factors, sector_factors,

            tech_enhanced_factors, microstructure_factors,
            
            fin_quality_factors, dividend_factors, corporate_factors,
            
            concept_factors, qv_composite_factors, ms_composite_factors,
            
            quality_factors, momentum_factors, liquidity_factors,

            analyst_factors, institution_factors, chip_factors,

            tech_pattern_factors, event_factors, money_flow_factors,

            volatility_factors, sentiment_factors, calendar_factors,

            governance_factors, analyst_rating_factors, sector_factors,

            tech_enhanced_factors, microstructure_factors,

            fin_quality_factors, dividend_factors, corporate_factors,

            concept_factors, qv_composite_factors, ms_composite_factors,

            im_composite_factors, dragon_factors,
            
            t0_rebound_factors

        )

        

        modules = [

            value_factors, profitability_factors, growth_factors,

            quality_factors, momentum_factors, liquidity_factors,

            analyst_factors, institution_factors, chip_factors,

            tech_pattern_factors, event_factors, money_flow_factors,

            volatility_factors, sentiment_factors, calendar_factors,

            governance_factors, analyst_rating_factors, sector_factors,

            tech_enhanced_factors, microstructure_factors,

            fin_quality_factors, dividend_factors, corporate_factors,

            concept_factors, qv_composite_factors, ms_composite_factors,

            im_composite_factors, dragon_factors,
            
            t0_rebound_factors

        ]

        

        for module in modules:

            try:

                registry.update(module.register())

            except Exception as e:

                logger.warning(f"注册因子模块失败 {module.__name__}: {e}")

        

        logger.info(f"因子注册表构建完 {len(registry)} 个因子")

        return registry

    

    def _prefetch_base_data(self, trade_date: str, stock_pool: list = None) -> dict:
        """预加载所有基础表数据，避免各模块重复查询 IO"""
        try:
            pool_filter = ""
            if stock_pool:
                pool_str = "','".join(stock_pool)
                pool_filter = f"AND ts_code IN ('{pool_str}')"

            logger.info("预加载基础数据表...")
            cache = {}

            # daily_basic
            cache['daily_basic'] = self.conn.execute(f"""
                SELECT * FROM daily_basic
                WHERE trade_date = '{trade_date}' {pool_filter}
            """).fetchdf()

            # balancesheet - 最新一期
            cache['balancesheet'] = self.conn.execute(f"""
                SELECT * FROM (
                    SELECT *, ROW_NUMBER() OVER (PARTITION BY ts_code ORDER BY ann_date DESC) AS rn
                    FROM balancesheet
                    WHERE ann_date <= '{trade_date}' {pool_filter}
                ) t WHERE rn = 1
            """).fetchdf()

            # income - 最新一期
            cache['income'] = self.conn.execute(f"""
                SELECT * FROM (
                    SELECT *, ROW_NUMBER() OVER (PARTITION BY ts_code ORDER BY ann_date DESC) AS rn
                    FROM income
                    WHERE ann_date <= '{trade_date}' {pool_filter}
                ) t WHERE rn = 1
            """).fetchdf()

            # cashflow - 最新一期
            cache['cashflow'] = self.conn.execute(f"""
                SELECT * FROM (
                    SELECT *, ROW_NUMBER() OVER (PARTITION BY ts_code ORDER BY ann_date DESC) AS rn
                    FROM cashflow
                    WHERE ann_date <= '{trade_date}' {pool_filter}
                ) t WHERE rn = 1
            """).fetchdf()

            # fina_indicator - 最新一期
            cache['fina_indicator'] = self.conn.execute(f"""
                SELECT * FROM (
                    SELECT *, ROW_NUMBER() OVER (PARTITION BY ts_code ORDER BY ann_date DESC) AS rn
                    FROM fina_indicator
                    WHERE ann_date <= '{trade_date}' {pool_filter}
                ) t WHERE rn = 1
            """).fetchdf()

            # stk_factor_pro - 当日技术指标
            cache['stk_factor_pro'] = self.conn.execute(f"""
                SELECT * FROM stk_factor_pro
                WHERE trade_date = '{trade_date}' {pool_filter}
            """).fetchdf()

            # stock_basic
            cache['stock_basic'] = self.conn.execute(f"""
                SELECT * FROM stock_basic
                WHERE 1=1 {pool_filter}
            """).fetchdf()

            logger.info(f"预加载完成: daily_basic={len(cache['daily_basic'])}行, "
                        f"fina_indicator={len(cache['fina_indicator'])}行, "
                        f"stk_factor_pro={len(cache['stk_factor_pro'])}行")
            return cache
        except Exception as e:
            logger.warning(f"预加载数据失败，将回退到各模块独立查询: {e}")
            return {}

    def compute_all(self, trade_date: str, stock_pool: list = None) -> pd.DataFrame:

        """计算全部208+因子，返回 DataFrame (index=ts_code, columns=factor_ids)"""

        import time
        all_results = {}
        timings = {}

        # 预加载基础表数据
        t_pre = time.time()
        prefetch_cache = self._prefetch_base_data(trade_date, stock_pool)
        timings['_prefetch'] = time.time() - t_pre

        # 按依赖顺序计算：基础维度 -> 复合维度

        base_dimensions = [p for p in DIMENSION_PREFIXES if p not in ('QV_', 'MS_', 'IM_')]

        composite_dimensions = ['QV_', 'MS_', 'IM_']

        # 并行计算所有独立基础维度（每个线程使用独立连接）
        def _compute_one(dim):
            t0 = time.time()
            try:
                thread_conn = duckdb.connect(self.db_path)
                try:
                    df = self._compute_dimension_with_conn(
                        dim, trade_date, stock_pool, thread_conn, prefetch_cache
                    )
                finally:
                    thread_conn.close()
                return dim, df, time.time() - t0
            except Exception as e:
                logger.error(f"计算维度{dim}失败: {e}")
                return dim, pd.DataFrame(), time.time() - t0

        logger.info(f"并行计算 {len(base_dimensions)} 个基础维度...")
        t_base = time.time()
        with ThreadPoolExecutor(max_workers=6) as executor:
            futures = {executor.submit(_compute_one, dim): dim for dim in base_dimensions}
            for future in as_completed(futures):
                dim, dim_df, elapsed = future.result()
                timings[dim] = elapsed
                if dim_df is not None and not dim_df.empty:
                    all_results.update(dim_df.to_dict())
        timings['_base_total'] = time.time() - t_base

        # 串行计算复合维度（依赖基础维度结果）
        logger.info("计算复合维度...")
        for dimension in composite_dimensions:
            t0 = time.time()
            try:
                dim_df = self._compute_dimension_with_conn(
                    dimension, trade_date, stock_pool, self.conn, prefetch_cache
                )
                if dim_df is not None and not dim_df.empty:
                    all_results.update(dim_df.to_dict())
            except Exception as e:
                logger.error(f"计算维度{dimension}失败: {e}")
            timings[dimension] = time.time() - t0

        # 打印耗时排行（降序）
        sorted_t = sorted(timings.items(), key=lambda x: -x[1])
        logger.info("===== 因子计算耗时排行 =====")
        for dim, elapsed in sorted_t:
            logger.info(f"  {dim:15s}: {elapsed:7.2f}s")
        logger.info(f"  {'总计':15s}: {sum(timings.values()):7.2f}s")

        if not all_results:
            return pd.DataFrame()

        return pd.DataFrame(all_results)

    

    def _compute_dimension_with_conn(self, dimension: str, trade_date: str,
                                     stock_pool: list, conn, prefetch_cache: dict) -> pd.DataFrame:
        """用指定连接计算某维度，支持 prefetch_cache 传入"""
        import inspect
        module_map = self._get_module_map()
        if dimension not in module_map:
            return pd.DataFrame()
        module = module_map[dimension]
        try:
            sig = inspect.signature(module.compute)
            if 'prefetch_cache' in sig.parameters and prefetch_cache:
                return module.compute(trade_date, conn, stock_pool, prefetch_cache=prefetch_cache)
            else:
                return module.compute(trade_date, conn, stock_pool)
        except Exception as e:
            logger.error(f"计算维度{dimension}失败: {e}")
            return pd.DataFrame()

    def compute_dimension(self, dimension: str, trade_date: str,

                          stock_pool: list = None, prefetch_cache: dict = None) -> pd.DataFrame:

        """计算指定维度的因子（外部调用入口，使用主连接）"""

        dim_factors = {k: v for k, v in self._registry.items()

                      if v['dimension'] == dimension}

        

        if not dim_factors:

            logger.warning(f"维度{dimension}无注册因子")

            return pd.DataFrame()

        return self._compute_dimension_with_conn(
            dimension, trade_date, stock_pool or [], self.conn, prefetch_cache or {}
        )

    

    def compute_single(self, factor_id: str, trade_date: str,

                       stock_pool: list = None) -> pd.Series:

        """计算单个因子"""

        info = self._registry.get(factor_id)

        if info is None:

            logger.warning(f"因子{factor_id}未注册")

            return pd.Series(dtype=float)

        

        dimension = info['dimension']

        dim_df = self.compute_dimension(dimension, trade_date, stock_pool)

        

        if factor_id in dim_df.columns:

            return dim_df[factor_id]

        return pd.Series(dtype=float)

    

    def get_factor_info(self, factor_id: str) -> dict:

        """获取因子元信息"""

        return self._registry.get(factor_id, {})

    

    def list_factors_by_dimension(self, dimension: str) -> list:

        """列出指定维度的所有因子ID"""

        return [k for k, v in self._registry.items() if v['dimension'] == dimension]

    

    def list_all_factors(self) -> list:

        """列出所有已注册因子ID"""

        return sorted(list(self._registry.keys()))

    

    def _get_module_map(self) -> dict:

        """维度前缀 -> 模块映射"""

        from backend.factors import (

            value_factors, profitability_factors, growth_factors,

            quality_factors, momentum_factors, liquidity_factors,

            analyst_factors, institution_factors, chip_factors,

            tech_pattern_factors, event_factors, money_flow_factors,

            volatility_factors, sentiment_factors, calendar_factors,

            governance_factors, analyst_rating_factors, sector_factors,

            tech_enhanced_factors, microstructure_factors,

            fin_quality_factors, dividend_factors, corporate_factors,

            concept_factors, qv_composite_factors, ms_composite_factors,

            im_composite_factors, dragon_factors,
            
            t0_rebound_factors

        )

        

        return {

            'V_': value_factors, 'P_': profitability_factors,

            'G_': growth_factors, 'Q_': quality_factors,

            'M_': momentum_factors, 'S_': liquidity_factors, 'L_': liquidity_factors,

            'E_': analyst_factors, 'I_': institution_factors,

            'C_': chip_factors, 'TP_': tech_pattern_factors,

            'EV_': event_factors, 'MF_': money_flow_factors,

            'VOL_': volatility_factors, 'SENT_': sentiment_factors,

            'CAL_': calendar_factors, 'GOV_': governance_factors,

            'ANALYST_': analyst_rating_factors, 'SECTOR_': sector_factors,

            'TECH_': tech_enhanced_factors, 'MICRO_': microstructure_factors,

            'FQ_': fin_quality_factors, 'DIV_': dividend_factors,

            'CORP_': corporate_factors, 'CONCEPT_': concept_factors,

            'QV_': qv_composite_factors, 'MS_': ms_composite_factors,

            'IM_': im_composite_factors, 'D_': dragon_factors,
            'T0_': t0_rebound_factors
        }
