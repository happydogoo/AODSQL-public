# -*- coding: utf-8 -*-
"""
查询优化器 - AODSQL系统的核心优化组件
实现基于成本的优化(CBO)和基于规则的优化(RBO)
"""

from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
from enum import Enum
import math
import time
import threading
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
from .logical_operators import LogicalOperator, LogicalPlan
from .logical_operators import ScanOperator, FilterOperator, IndexScanOperator, JoinOperator
from .symbol_table import SymbolTable, TableInfo
from src.engine.catalog_manager import CatalogManager
from .enhanced_query_planner import EnhancedQueryPlanner

class OptimizationStrategy(Enum):
    """优化策略枚举"""
    RULE_BASED = "rule_based"      # 基于规则的优化
    COST_BASED = "cost_based"      # 基于成本的优化
    HYBRID = "hybrid"              # 混合优化策略

class JoinMethod(Enum):
    """连接方法枚举"""
    NESTED_LOOP = "nested_loop"    # 嵌套循环连接
    HASH_JOIN = "hash_join"        # 哈希连接
    SORT_MERGE = "sort_merge"      # 排序合并连接


class ParallelStrategy(Enum):
    """并行策略枚举"""
    SEQUENTIAL = "sequential"      # 顺序执行
    PARALLEL_SCAN = "parallel_scan" # 并行扫描
    PARALLEL_JOIN = "parallel_join" # 并行连接
    PARALLEL_OPTIMIZATION = "parallel_optimization" # 并行优化

@dataclass
class CostInfo:
    """成本信息"""
    io_cost: float = 0.0          # I/O成本
    cpu_cost: float = 0.0         # CPU成本
    memory_cost: float = 0.0      # 内存成本
    total_cost: float = 0.0       # 总成本
    
    def __post_init__(self):
        # 增加索引优化权重系数
        self.total_cost = self.io_cost * 0.7 + self.cpu_cost * 0.25 + self.memory_cost * 0.05

@dataclass
class Statistics:
    """统计信息"""
    table_name: str
    row_count: int = 0
    page_count: int = 0
    avg_row_size: float = 0.0
    column_stats: Dict[str, 'ColumnStatistics'] = None
    
    def __post_init__(self):
        if self.column_stats is None:
            self.column_stats = {}

@dataclass
class ColumnStatistics:
    """列统计信息"""
    column_name: str
    distinct_count: int = 0
    null_count: int = 0
    min_value: Any = None
    max_value: Any = None
    most_common_values: List[Tuple[Any, int]] = None
    histogram: List[Tuple[Any, int]] = None  # 直方图统计
    correlation_with_other_columns: Dict[str, float] = None  # 与其他列的相关性
    
    def __post_init__(self):
        if self.most_common_values is None:
            self.most_common_values = []
        if self.histogram is None:
            self.histogram = []
        if self.correlation_with_other_columns is None:
            self.correlation_with_other_columns = {}

@dataclass
class EnhancedStatistics:
    """增强的统计信息"""
    table_name: str
    row_count: int = 0
    page_count: int = 0
    avg_row_size: float = 0.0
    column_stats: Dict[str, ColumnStatistics] = None
    last_updated: float = 0.0  # 最后更新时间戳
    update_frequency: int = 1000  # 更新频率（每1000次操作更新一次）
    operation_count: int = 0  # 操作计数器
    
    def __post_init__(self):
        if self.column_stats is None:
            self.column_stats = {}
        if self.last_updated == 0.0:
            self.last_updated = time.time()

@dataclass
class ParallelExecutionInfo:
    """并行执行信息"""
    strategy: ParallelStrategy = ParallelStrategy.SEQUENTIAL
    max_workers: int = 4
    estimated_parallelism: float = 1.0
    parallel_cost_reduction: float = 0.0
    
@dataclass
class OptimizationResult:
    """优化结果"""
    plan: LogicalPlan
    cost: CostInfo
    parallel_info: Optional[ParallelExecutionInfo] = None
    optimization_time: float = 0.0

@dataclass
class AdaptiveParameters:
    """自适应参数"""
    io_cost_per_page: float = 1.0
    cpu_cost_per_row: float = 0.001
    memory_cost_per_page: float = 0.1
    index_seek_cost_per_level: float = 0.1
    index_scan_cost_per_row: float = 0.01
    index_fetch_cost_per_page: float = 0.1
    index_cpu_cost_per_row: float = 0.0001
    index_memory_cost: float = 0.05
    
    # 自适应调整因子
    io_adjustment_factor: float = 1.0
    cpu_adjustment_factor: float = 1.0
    memory_adjustment_factor: float = 1.0
    
    def apply_adjustments(self):
        """应用调整因子"""
        self.io_cost_per_page *= self.io_adjustment_factor
        self.cpu_cost_per_row *= self.cpu_adjustment_factor
        self.memory_cost_per_page *= self.memory_adjustment_factor
        self.index_seek_cost_per_level *= self.cpu_adjustment_factor
        self.index_scan_cost_per_row *= self.cpu_adjustment_factor
        self.index_fetch_cost_per_page *= self.io_adjustment_factor
        self.index_cpu_cost_per_row *= self.cpu_adjustment_factor
        self.index_memory_cost *= self.memory_adjustment_factor

class StatisticsCollector:
    """统计信息收集器"""
    
    def __init__(self, catalog_manager: CatalogManager = None):
        self.catalog = catalog_manager
        self.collection_cache = {}  # 收集缓存
        self.collection_lock = threading.Lock()  # 线程安全锁
    
    def collect_table_statistics(self, table_name: str, force_refresh: bool = False) -> EnhancedStatistics:
        """收集表统计信息"""
        with self.collection_lock:
            # 检查缓存
            if not force_refresh and table_name in self.collection_cache:
                cached_stats = self.collection_cache[table_name]
                if time.time() - cached_stats.last_updated < 3600:  # 1小时内不重新收集
                    return cached_stats
            
            # 从catalog获取基础信息
            table_info = self.catalog.get_table(table_name) if self.catalog else None
            
            # 创建增强统计信息
            stats = EnhancedStatistics(
                table_name=table_name,
                row_count=getattr(table_info, 'row_count', 0) if table_info else 0,
                page_count=getattr(table_info, 'page_count', 0) if table_info else 0,
                avg_row_size=self._estimate_avg_row_size(table_info) if table_info else 0.0,
                last_updated=time.time()
            )
            
            # 收集列统计信息
            if table_info and hasattr(table_info, 'columns'):
                for column in table_info.columns:
                    col_stats = self._collect_column_statistics(table_name, column.column_name)
                    stats.column_stats[column.column_name] = col_stats
            
            # 更新缓存
            self.collection_cache[table_name] = stats
            return stats
    
    def _collect_column_statistics(self, table_name: str, column_name: str) -> ColumnStatistics:
        """收集列统计信息"""
        col_stats = ColumnStatistics(column_name=column_name)
        
        if self.catalog:
            try:
                # 从catalog获取列统计信息
                catalog_col_stats = self.catalog.get_column_stats(table_name, column_name)
                if catalog_col_stats:
                    col_stats.distinct_count = catalog_col_stats.get('distinct', 0)
                    col_stats.null_count = catalog_col_stats.get('null_count', 0)
                    col_stats.min_value = catalog_col_stats.get('min')
                    col_stats.max_value = catalog_col_stats.get('max')
                    col_stats.most_common_values = catalog_col_stats.get('mcv', [])
                    col_stats.histogram = catalog_col_stats.get('histogram', [])
            except Exception as e:
                # 如果获取失败，使用默认值
                pass
        
        return col_stats
    
    def _estimate_avg_row_size(self, table_info) -> float:
        """估算平均行大小"""
        if not table_info or not hasattr(table_info, 'columns'):
            return 100.0  # 默认行大小
        
        total_size = 0
        for column in table_info.columns:
            if hasattr(column, 'data_type'):
                if column.data_type == 'INT':
                    total_size += 4
                elif column.data_type == 'VARCHAR':
                    total_size += 50  # 假设平均50字符
                elif column.data_type == 'DECIMAL':
                    total_size += 8
                else:
                    total_size += 4
        
        return float(total_size)
    
    def update_statistics_after_operation(self, table_name: str, operation_type: str, affected_rows: int = 0):
        """在操作后更新统计信息"""
        with self.collection_lock:
            if table_name in self.collection_cache:
                stats = self.collection_cache[table_name]
                stats.operation_count += 1
                
                # 根据操作类型更新统计信息
                if operation_type == 'INSERT':
                    stats.row_count += affected_rows
                elif operation_type == 'DELETE':
                    stats.row_count = max(0, stats.row_count - affected_rows)
                elif operation_type == 'UPDATE':
                    # UPDATE不改变行数，但可能需要更新列统计信息
                    pass
                
                # 检查是否需要重新收集统计信息
                if stats.operation_count >= stats.update_frequency:
                    self.collect_table_statistics(table_name, force_refresh=True)
    
    def get_statistics_summary(self) -> Dict[str, Any]:
        """获取统计信息摘要"""
        with self.collection_lock:
            summary = {
                'cached_tables': len(self.collection_cache),
                'tables': {}
            }
            
            for table_name, stats in self.collection_cache.items():
                summary['tables'][table_name] = {
                    'row_count': stats.row_count,
                    'page_count': stats.page_count,
                    'avg_row_size': stats.avg_row_size,
                    'last_updated': stats.last_updated,
                    'operation_count': stats.operation_count,
                    'columns_count': len(stats.column_stats)
                }
            
            return summary

class QueryOptimizer:
    """查询优化器"""
    
    def __init__(self, symbol_table: SymbolTable, strategy: OptimizationStrategy = OptimizationStrategy.HYBRID, catalog_manager: CatalogManager = None, enable_optimization: bool = True, enable_parallel: bool = True):
        self.symbol_table = symbol_table
        self.strategy = strategy
        self.statistics = {}  # 表统计信息
        self.catalog = catalog_manager
        self.card_estimator = CardinalityEstimator(catalog_manager)
        
        # 演示模式开关
        self.enable_optimization = enable_optimization
        self.enable_parallel = enable_parallel
        self.demo_mode = False  # 演示模式标志
        
        # 并行执行配置
        self.max_workers = 4
        self.parallel_threshold = 1000  # 超过1000行的表才考虑并行
        
        # 自适应参数调优
        self.adaptive_params = AdaptiveParameters()
        self.enable_adaptive_tuning = True
        self.performance_history = []  # 存储性能历史用于自适应调优
        
        # 增强统计信息收集
        self.statistics_collector = StatisticsCollector(catalog_manager)
        self.enable_enhanced_statistics = True
        
        # 初始化成本模型和规则引擎（在自适应参数之后）
        self.cost_model = CostModel(catalog_manager, self.adaptive_params)  # 传递catalog和自适应参数给成本模型
        self.rule_engine = RuleEngine(catalog_manager, statistics_supplier=lambda: self.statistics)
        
        # 初始化统计信息
        self._initialize_statistics()
    
    def optimize(self, logical_plan: LogicalPlan) -> LogicalPlan:
        """
        优化逻辑执行计划
        输入: 原始逻辑计划
        输出: 优化后的逻辑计划
        """
        start_time = time.time()
        
        # 如果禁用优化，直接返回原始计划
        if not self.enable_optimization:
            return logical_plan
            
        # 只在演示模式下打印详细信息
        if self.demo_mode:
            print(f"🔧 开始查询优化 (策略: {self.strategy.value})")
        
        # 1. 查询重写
        rewritten_plan = self._rewrite_query(logical_plan)
        if self.demo_mode:
            print("   ✅ 查询重写完成")
        
        # 2. 基于规则的优化
        if self.strategy in [OptimizationStrategy.RULE_BASED, OptimizationStrategy.HYBRID]:
            rule_optimized_plan = self._rule_based_optimization(rewritten_plan)
            if self.demo_mode:
                print("   ✅ 基于规则的优化完成")
        else:
            rule_optimized_plan = rewritten_plan
        
        # 3. 基于成本的优化（支持并行）
        if self.strategy in [OptimizationStrategy.COST_BASED, OptimizationStrategy.HYBRID]:
            if self.enable_parallel:
                cost_optimized_plan = self._parallel_cost_based_optimization(rule_optimized_plan)
            else:
                cost_optimized_plan = self._cost_based_optimization(rule_optimized_plan)
            if self.demo_mode:
                print("   ✅ 基于成本的优化完成")
        else:
            cost_optimized_plan = rule_optimized_plan
        
        # 4. 为优化后的计划添加成本估算信息
        self._add_cost_estimates(cost_optimized_plan)
        
        # 5. 生成优化报告（仅在演示模式）
        if self.demo_mode:
            optimization_time = time.time() - start_time
            self._generate_optimization_report(logical_plan, cost_optimized_plan, optimization_time)
            self._show_optimization_decisions()
        
        return cost_optimized_plan
    
    def _add_cost_estimates(self, plan: LogicalPlan):
        """为逻辑计划添加成本估算信息"""
        if not plan or not plan.root:
            return
        
        # 计算每个操作符的成本和行数估算
        self._calculate_operator_costs(plan.root)
    
    def _calculate_operator_costs(self, operator: LogicalOperator):
        """递归计算操作符的成本和行数估算"""
        if not operator:
            return
        
        # 计算当前操作符的成本
        cost_info = self.cost_model.calculate_cost(LogicalPlan(operator), self.statistics)
        
        # 设置估算信息
        operator.estimated_cost = cost_info.total_cost
        operator.estimated_rows = self._estimate_operator_rows(operator)
        
        # 递归处理子操作符
        if hasattr(operator, 'children') and operator.children:
            for child in operator.children:
                self._calculate_operator_costs(child)
        elif hasattr(operator, 'left_child') and operator.left_child:
            self._calculate_operator_costs(operator.left_child)
        if hasattr(operator, 'right_child') and operator.right_child:
            self._calculate_operator_costs(operator.right_child)
    
    def _estimate_operator_rows(self, operator: LogicalOperator) -> int:
        """估算操作符的输出行数"""
        if hasattr(operator, 'table_name'):
            # 扫描操作符
            table_name = operator.table_name
            if table_name in self.statistics:
                return self.statistics[table_name].row_count
            return 1000  # 默认估算
        
        if hasattr(operator, 'operator_type'):
            op_type = operator.operator_type.value
            if op_type == 'Filter':
                # 过滤操作符，假设选择性为0.1
                if hasattr(operator, 'children') and operator.children:
                    child_rows = self._estimate_operator_rows(operator.children[0])
                    return max(1, int(child_rows * 0.1))
            elif op_type == 'Project':
                # 投影操作符，行数不变
                if hasattr(operator, 'children') and operator.children:
                    return self._estimate_operator_rows(operator.children[0])
        
        return 1000  # 默认估算
    
    def _show_optimization_decisions(self):
        """显示优化决策过程"""
        print("\n🔍 优化决策详情:")
        print("   " + "="*50)
        
        decisions = self.rule_engine.get_decisions()
        if decisions:
            for i, decision in enumerate(decisions, 1):
                print(f"   决策 {i}:")
                print(f"     表: {decision.get('table', 'N/A')}")
                print(f"     列: {decision.get('column', 'N/A')}")
                print(f"     谓词: {decision.get('predicate', 'N/A')}")
                print(f"     操作符: {decision.get('operator', 'N/A')}")
                print(f"     选择性: {decision.get('selectivity_estimate', 0):.4f}")
                print(f"     全表扫描成本: {decision.get('seq_cost_estimate', 0):.2f}")
                print(f"     索引扫描成本: {decision.get('index_cost_estimate', 0):.2f}")
                print(f"     选择: {decision.get('chosen', 'N/A')}")
                print(f"     索引名: {decision.get('index_name', 'N/A')}")
                print()
        else:
            print("   没有找到索引选择决策")
        
        print("   " + "="*50)
    
    def _rewrite_query(self, plan: LogicalPlan) -> LogicalPlan:
        """查询重写"""
        # 1. 消除冗余子查询
        # 2. 简化表达式
        # 3. 谓词下推
        # 4. 列裁剪
        return plan
    
    def _rule_based_optimization(self, plan: LogicalPlan) -> LogicalPlan:
        """基于规则的优化"""
        return self.rule_engine.optimize(plan)
    
    def _cost_based_optimization(self, plan: LogicalPlan) -> LogicalPlan:
        """基于成本的优化"""
        # 1. 生成候选执行计划
        candidate_plans = self._generate_candidate_plans(plan)
        
        # 2. 计算每个计划的成本
        best_plan = None
        best_cost = float('inf')
        
        for candidate_plan in candidate_plans:
            cost = self.cost_model.calculate_cost(candidate_plan, self.statistics)
            if self.demo_mode:
                print(f"   📊 候选计划成本: {cost.total_cost:.2f}")
            
            if cost.total_cost < best_cost:
                best_cost = cost.total_cost
                best_plan = candidate_plan
        
        if self.demo_mode:
            print(f"   🏆 最优计划成本: {best_cost:.2f}")
        return best_plan or plan
    
    def _parallel_cost_based_optimization(self, plan: LogicalPlan) -> LogicalPlan:
        """并行基于成本的优化"""
        if self.demo_mode:
            print("   🚀 启用并行优化")
        
        # 1. 生成候选执行计划
        candidate_plans = self._generate_candidate_plans(plan)
        
        # 2. 并行计算每个计划的成本
        best_plan = None
        best_cost = float('inf')
        
        if len(candidate_plans) > 1 and self.enable_parallel:
            # 使用线程池并行计算成本
            with ThreadPoolExecutor(max_workers=min(self.max_workers, len(candidate_plans))) as executor:
                # 提交所有成本计算任务
                future_to_plan = {
                    executor.submit(self.cost_model.calculate_cost, candidate_plan, self.statistics): candidate_plan
                    for candidate_plan in candidate_plans
                }
                
                # 收集结果
                for future in concurrent.futures.as_completed(future_to_plan):
                    candidate_plan = future_to_plan[future]
                    try:
                        cost = future.result()
                        if self.demo_mode:
                            print(f"   📊 候选计划成本: {cost.total_cost:.2f}")
                        
                        if cost.total_cost < best_cost:
                            best_cost = cost.total_cost
                            best_plan = candidate_plan
                    except Exception as e:
                        if self.demo_mode:
                            print(f"   ⚠️ 成本计算失败: {e}")
        else:
            # 顺序计算成本
            for candidate_plan in candidate_plans:
                cost = self.cost_model.calculate_cost(candidate_plan, self.statistics)
                if self.demo_mode:
                    print(f"   📊 候选计划成本: {cost.total_cost:.2f}")
                
                if cost.total_cost < best_cost:
                    best_cost = cost.total_cost
                    best_plan = candidate_plan
        
        if self.demo_mode:
            print(f"   🏆 最优计划成本: {best_cost:.2f}")
        
        return best_plan or plan
    
    def adaptive_tune_parameters(self, actual_performance: Dict[str, float]):
        """自适应调优参数"""
        if not self.enable_adaptive_tuning:
            return
        
        # 记录性能历史
        self.performance_history.append({
            'timestamp': time.time(),
            'performance': actual_performance,
            'params': {
                'io_cost_per_page': self.adaptive_params.io_cost_per_page,
                'cpu_cost_per_row': self.adaptive_params.cpu_cost_per_row,
                'memory_cost_per_page': self.adaptive_params.memory_cost_per_page
            }
        })
        
        # 保持最近100条记录
        if len(self.performance_history) > 100:
            self.performance_history = self.performance_history[-100:]
        
        # 分析性能趋势并调整参数
        if len(self.performance_history) >= 10:
            self._analyze_and_adjust_parameters()
    
    def _analyze_and_adjust_parameters(self):
        """分析性能趋势并调整参数"""
        recent_performance = self.performance_history[-10:]
        
        # 计算平均性能指标
        avg_io_time = sum(p['performance'].get('io_time', 0) for p in recent_performance) / len(recent_performance)
        avg_cpu_time = sum(p['performance'].get('cpu_time', 0) for p in recent_performance) / len(recent_performance)
        avg_memory_usage = sum(p['performance'].get('memory_usage', 0) for p in recent_performance) / len(recent_performance)
        
        # 与历史平均值比较
        if len(self.performance_history) >= 20:
            historical_avg_io = sum(p['performance'].get('io_time', 0) for p in self.performance_history[-20:-10]) / 10
            historical_avg_cpu = sum(p['performance'].get('cpu_time', 0) for p in self.performance_history[-20:-10]) / 10
            historical_avg_memory = sum(p['performance'].get('memory_usage', 0) for p in self.performance_history[-20:-10]) / 10
            
            # 调整I/O成本参数
            if avg_io_time > historical_avg_io * 1.1:  # I/O时间增加超过10%
                self.adaptive_params.io_adjustment_factor *= 1.05  # 增加I/O成本权重
            elif avg_io_time < historical_avg_io * 0.9:  # I/O时间减少超过10%
                self.adaptive_params.io_adjustment_factor *= 0.95  # 减少I/O成本权重
            
            # 调整CPU成本参数
            if avg_cpu_time > historical_avg_cpu * 1.1:  # CPU时间增加超过10%
                self.adaptive_params.cpu_adjustment_factor *= 1.05  # 增加CPU成本权重
            elif avg_cpu_time < historical_avg_cpu * 0.9:  # CPU时间减少超过10%
                self.adaptive_params.cpu_adjustment_factor *= 0.95  # 减少CPU成本权重
            
            # 调整内存成本参数
            if avg_memory_usage > historical_avg_memory * 1.1:  # 内存使用增加超过10%
                self.adaptive_params.memory_adjustment_factor *= 1.05  # 增加内存成本权重
            elif avg_memory_usage < historical_avg_memory * 0.9:  # 内存使用减少超过10%
                self.adaptive_params.memory_adjustment_factor *= 0.95  # 减少内存成本权重
            
            # 应用调整
            self.adaptive_params.apply_adjustments()
            
            # 更新成本模型
            self.cost_model._apply_adaptive_parameters()
            
            if self.demo_mode:
                print(f"   🔧 自适应参数调优:")
                print(f"     I/O调整因子: {self.adaptive_params.io_adjustment_factor:.3f}")
                print(f"     CPU调整因子: {self.adaptive_params.cpu_adjustment_factor:.3f}")
                print(f"     内存调整因子: {self.adaptive_params.memory_adjustment_factor:.3f}")
    
    def get_adaptive_parameters_info(self) -> Dict[str, Any]:
        """获取自适应参数信息"""
        return {
            'io_cost_per_page': self.adaptive_params.io_cost_per_page,
            'cpu_cost_per_row': self.adaptive_params.cpu_cost_per_row,
            'memory_cost_per_page': self.adaptive_params.memory_cost_per_page,
            'io_adjustment_factor': self.adaptive_params.io_adjustment_factor,
            'cpu_adjustment_factor': self.adaptive_params.cpu_adjustment_factor,
            'memory_adjustment_factor': self.adaptive_params.memory_adjustment_factor,
            'performance_history_size': len(self.performance_history)
        }
    
    def update_statistics_after_operation(self, table_name: str, operation_type: str, affected_rows: int = 0):
        """在操作后更新统计信息"""
        if self.enable_enhanced_statistics:
            self.statistics_collector.update_statistics_after_operation(table_name, operation_type, affected_rows)
            
            # 重新收集统计信息
            enhanced_stats = self.statistics_collector.collect_table_statistics(table_name)
            
            # 更新内部统计信息
            if table_name in self.statistics:
                self.statistics[table_name].row_count = enhanced_stats.row_count
                self.statistics[table_name].page_count = enhanced_stats.page_count
                self.statistics[table_name].avg_row_size = enhanced_stats.avg_row_size
                
                # 更新列统计信息
                for col_name, col_stats in enhanced_stats.column_stats.items():
                    self.statistics[table_name].column_stats[col_name] = col_stats
    
    def get_statistics_summary(self) -> Dict[str, Any]:
        """获取统计信息摘要"""
        summary = {
            'enhanced_statistics_enabled': self.enable_enhanced_statistics,
            'statistics_count': len(self.statistics)
        }
        
        if self.enable_enhanced_statistics:
            summary['statistics_collector'] = self.statistics_collector.get_statistics_summary()
        
        return summary
    
    def refresh_statistics(self, table_name: str = None):
        """刷新统计信息"""
        if table_name:
            # 刷新特定表的统计信息
            if self.enable_enhanced_statistics:
                enhanced_stats = self.statistics_collector.collect_table_statistics(table_name, force_refresh=True)
                
                # 更新内部统计信息
                if table_name in self.statistics:
                    self.statistics[table_name].row_count = enhanced_stats.row_count
                    self.statistics[table_name].page_count = enhanced_stats.page_count
                    self.statistics[table_name].avg_row_size = enhanced_stats.avg_row_size
                    
                    # 更新列统计信息
                    for col_name, col_stats in enhanced_stats.column_stats.items():
                        self.statistics[table_name].column_stats[col_name] = col_stats
        else:
            # 刷新所有表的统计信息
            self._initialize_statistics()
    
    def _generate_candidate_plans(self, plan: LogicalPlan) -> List[LogicalPlan]:
        """生成候选执行计划"""
        candidates = [plan]  # 原始计划
        
        # 1. 索引选择优化 - 为每个Scan操作符生成索引扫描候选
        index_candidates = self._generate_index_scan_candidates(plan)
        if index_candidates:  # 只有在有索引候选时才添加
            candidates.extend(index_candidates)
        
        # 2. 连接顺序优化（暂时跳过，减少开销）
        # candidates.extend(self._generate_join_order_candidates(plan))
        
        # 3. 连接方法优化
        join_method_candidates = self._generate_join_method_candidates(plan)
        if join_method_candidates:
            candidates.extend(join_method_candidates)
        
        return candidates
    
    def _generate_index_scan_candidates(self, plan: LogicalPlan) -> List[LogicalPlan]:
        """生成索引扫描候选计划"""
        candidates = []
        
        # 遍历计划树，找到Scan操作符
        def find_scans(operator):
            if hasattr(operator, 'operator_type') and operator.operator_type.value == 'Scan':
                # 检查是否有可用的索引
                table_name = getattr(operator, 'table_name', '')
                if self.catalog and table_name:
                    table_info = self.catalog.get_table(table_name)
                    if table_info and table_info.indexes:
                        # 为每个索引创建候选计划
                        for index_name, index_info in table_info.indexes.items():
                            # 获取索引列名
                            if isinstance(index_info, dict):
                                column_name = index_info.get('columns', [index_name])[0]
                            else:
                                column_name = getattr(index_info, 'columns', [index_name])[0]
                            
                            # 查找相关的过滤条件
                            predicate = self._find_predicate_for_column(plan.root, table_name, column_name)
                            
                            # 只有在有相关过滤条件时才考虑索引扫描
                            if not predicate:
                                continue
                            
                            # 计算选择性
                            selectivity = self._calculate_selectivity(table_name, column_name, predicate)
                            
                            # 创建索引扫描操作符
                            idx_scan = IndexScanOperator(
                                table_name=table_name,
                                index_name=index_name,
                                column_name=column_name,
                                predicate=predicate
                            )
                            
                            # 设置选择性属性
                            idx_scan.selectivity = selectivity
                            
                            # 替换原始计划中的Scan操作符
                            new_plan = self._replace_operator_in_plan(plan, operator, idx_scan)
                            if new_plan:
                                candidates.append(new_plan)
            
            # 递归处理子操作符
            if hasattr(operator, 'children'):
                for child in operator.children:
                    find_scans(child)
        
        find_scans(plan.root)
        return candidates
    
    def _find_predicate_for_column(self, operator, table_name: str, column_name: str) -> Optional[str]:
        """查找与指定列相关的谓词条件"""
        if hasattr(operator, 'operator_type'):
            op_type = operator.operator_type.value
            if op_type == 'Filter':
                # 检查过滤条件是否涉及指定列
                condition = getattr(operator, 'condition', None)
                if condition and column_name in str(condition):
                    return str(condition)
            
            # 递归检查子操作符
            if hasattr(operator, 'children'):
                for child in operator.children:
                    predicate = self._find_predicate_for_column(child, table_name, column_name)
                    if predicate:
                        return predicate
        return None
    
    def _calculate_selectivity(self, table_name: str, column_name: str, predicate: Optional[str]) -> float:
        """计算列的选择性"""
        if not predicate:
            return 0.5  # 默认选择性
        
        # 尝试从统计信息获取真实的选择性
        if self.catalog:
            try:
                col_stats = self.catalog.get_column_stats(table_name, column_name)
                if col_stats and 'distinct' in col_stats:
                    distinct_count = col_stats['distinct']
                    if '=' in predicate or '==' in predicate:
                        # 等值查询：使用真实distinct值
                        return 1.0 / max(1, distinct_count)
                    elif any(op in predicate for op in ['<', '>', '<=', '>=']):
                        # 范围查询：基于distinct值估算
                        return max(0.01, min(0.5, 1.0 / max(1, distinct_count) * 10))
            except Exception:
                pass
        
        # 如果无法获取真实统计，使用默认估算
        if '=' in predicate or '==' in predicate:
            return 0.1  # 等值查询，假设10%选择性
        elif any(op in predicate for op in ['<', '>', '<=', '>=']):
            return 0.1  # 范围查询，中等选择性
        else:
            return 0.5  # 其他情况，低选择性
    
    def _generate_join_order_candidates(self, plan: LogicalPlan) -> List[LogicalPlan]:
        """生成连接顺序候选计划"""
        # 获取所有连接节点
        join_nodes = []
        self._collect_join_nodes(plan, join_nodes)
        
        if len(join_nodes) <= 1:
            return [plan]
        
        # 生成不同的连接顺序
        candidates = []
        tables = [node.table_name for node in join_nodes]
        
        # 生成所有可能的连接顺序排列
        from itertools import permutations
        for perm in permutations(tables):
            if perm != tuple(tables):  # 排除原始顺序
                new_plan = self._reorder_joins(plan, perm)
                if new_plan:
                    candidates.append(new_plan)
        
        return candidates
    
    def _collect_join_nodes(self, plan: LogicalPlan, join_nodes: List):
        """递归收集所有连接节点"""
        if hasattr(plan.root, 'left_child') and plan.root.left_child:
            left_plan = LogicalPlan(plan.root.left_child)
            self._collect_join_nodes(left_plan, join_nodes)
        if hasattr(plan.root, 'right_child') and plan.root.right_child:
            right_plan = LogicalPlan(plan.root.right_child)
            self._collect_join_nodes(right_plan, join_nodes)
        if plan.node_type == 'Join':
            join_nodes.append(plan)
    
    def _reorder_joins(self, plan: LogicalPlan, new_order: tuple) -> Optional[LogicalPlan]:
        """重新排序连接节点"""
        try:
            # 创建新的连接计划
            new_plan = LogicalPlan('Join')
            new_plan.left_child = LogicalPlan('SeqScan')
            new_plan.left_child.table_name = new_order[0]
            
            current = new_plan
            for i in range(1, len(new_order)):
                current.right_child = LogicalPlan('SeqScan')
                current.right_child.table_name = new_order[i]
                if i < len(new_order) - 1:
                    current = LogicalPlan('Join')
                    current.left_child = current.right_child
            
            return new_plan
        except Exception as e:
            self.logger.warning(f"无法重新排序连接: {e}")
            return None
    
    def _generate_join_method_candidates(self, plan: LogicalPlan) -> List[LogicalPlan]:
        """生成连接方法候选计划"""
        candidates = []
        
        # 查找所有连接操作符
        join_operators = self._find_join_operators(plan.root)
        
        for join_op in join_operators:
            # 为每个连接操作符生成不同的连接方法候选
            for join_method in [JoinMethod.HASH_JOIN, JoinMethod.SORT_MERGE]:
                if self._is_join_method_applicable(join_op, join_method):
                    new_plan = self._create_join_method_candidate(plan, join_op, join_method)
                    if new_plan:
                        candidates.append(new_plan)
        
        return candidates
    
    def _find_join_operators(self, operator) -> List:
        """递归查找所有连接操作符"""
        join_ops = []
        
        if hasattr(operator, 'operator_type') and operator.operator_type.value == 'Join':
            join_ops.append(operator)
        
        # 递归查找子操作符
        if hasattr(operator, 'children') and operator.children:
            for child in operator.children:
                join_ops.extend(self._find_join_operators(child))
        
        return join_ops
    
    def _is_join_method_applicable(self, join_op, join_method: JoinMethod) -> bool:
        """判断连接方法是否适用于给定的连接操作符"""
        if not hasattr(join_op, 'left_child') or not hasattr(join_op, 'right_child'):
            return False
        
        # 获取左右子表的统计信息
        left_stats = self._get_table_stats_from_operator(join_op.left_child)
        right_stats = self._get_table_stats_from_operator(join_op.right_child)
        
        if not left_stats or not right_stats:
            return False
        
        if join_method == JoinMethod.HASH_JOIN:
            # 哈希连接适用于：小表作为构建表，大表作为探测表
            # 或者内存足够容纳较小的表
            smaller_table = min(left_stats.row_count, right_stats.row_count)
            return smaller_table < 10000  # 假设内存可以容纳10000行
        
        elif join_method == JoinMethod.SORT_MERGE:
            # 排序合并连接适用于：数据已经有序或接近有序
            # 或者连接条件涉及范围比较
            condition = getattr(join_op, 'condition', '')
            return any(op in str(condition) for op in ['<', '>', '<=', '>='])
        
        return True
    
    def _get_table_stats_from_operator(self, operator) -> Optional[Statistics]:
        """从操作符获取表统计信息"""
        if hasattr(operator, 'table_name'):
            table_name = operator.table_name
            return self.statistics.get(table_name)
        return None
    
    def _create_join_method_candidate(self, original_plan: LogicalPlan, join_op, join_method: JoinMethod) -> Optional[LogicalPlan]:
        """创建使用特定连接方法的候选计划"""
        try:
            # 创建新的连接操作符
            new_join_op = JoinOperator(
                join_type=join_op.join_type,
                condition=join_op.condition,
                join_method=join_method
            )
            
            # 复制子操作符
            if hasattr(join_op, 'left_child'):
                new_join_op.left_child = join_op.left_child
            if hasattr(join_op, 'right_child'):
                new_join_op.right_child = join_op.right_child
            
            # 创建新的计划
            new_plan = LogicalPlan(new_join_op)
            return new_plan
        except Exception as e:
            if self.demo_mode:
                print(f"   ⚠️ 创建连接方法候选失败: {e}")
            return None
    
    def _replace_operator_in_plan(self, plan: LogicalPlan, old_op, new_op) -> LogicalPlan:
        """在计划中替换操作符"""
        # 简化实现：创建新的计划
        try:
            # 这里需要深度复制计划并替换操作符
            # 为了简化，我们创建一个新的计划
            return LogicalPlan(new_op)
        except:
            return None
    
    def _initialize_statistics(self):
        """初始化统计信息"""
        for table_name, table_info in self.symbol_table.tables.items():
            if self.enable_enhanced_statistics:
                # 使用增强统计信息收集器
                enhanced_stats = self.statistics_collector.collect_table_statistics(table_name)
                
                # 转换为兼容的Statistics格式
                stats = Statistics(
                    table_name=table_name,
                    row_count=enhanced_stats.row_count,
                    page_count=enhanced_stats.page_count,
                    avg_row_size=enhanced_stats.avg_row_size
                )
                
                # 转换列统计信息
                for col_name, col_stats in enhanced_stats.column_stats.items():
                    stats.column_stats[col_name] = col_stats
                
                self.statistics[table_name] = stats
            else:
                # 使用原有的简化统计信息收集
                stats = Statistics(
                    table_name=table_name,
                    row_count=self._estimate_row_count(table_name),
                    page_count=self._estimate_page_count(table_name),
                    avg_row_size=self._estimate_avg_row_size(table_info)
                )
                
                # 为每列生成统计信息
                for column in table_info.columns:
                    col_stats = ColumnStatistics(
                        column_name=column.name,
                        distinct_count=self._estimate_distinct_count(table_name, column.name),
                        null_count=0,  # 简化实现
                        min_value=None,
                        max_value=None
                    )
                    stats.column_stats[column.name] = col_stats
                
                self.statistics[table_name] = stats
    
    def _estimate_row_count(self, table_name: str) -> int:
        """优先使用 Catalog 真实统计，否则使用默认估算"""
        if hasattr(self, 'catalog') and self.catalog is not None:
            try:
                t = self.catalog.get_table(table_name)
                catalog_row_count = int(getattr(t, 'row_count', 0))
                if catalog_row_count > 0:
                    return catalog_row_count
            except Exception:
                pass
        # 回退估算 - 使用更真实的默认值
        if 'users' in table_name.lower():
            return 10000
        elif 'orders' in table_name.lower():
            return 100000
        elif 'products' in table_name.lower():
            return 5000
        elif 'customers' in table_name.lower():
            return 50000
        elif 'test_table' in table_name.lower():
            return 1000
        else:
            return 1000
    
    def _estimate_page_count(self, table_name: str) -> int:
        """优先使用 Catalog 真实统计，否则按行数估算页数"""
        if hasattr(self, 'catalog') and self.catalog is not None:
            try:
                t = self.catalog.get_table(table_name)
                pc = int(getattr(t, 'page_count', 0))
                if pc > 0:
                    return pc
            except Exception:
                pass
        row_count = self._estimate_row_count(table_name)
        rows_per_page = 100  # 假设每页100行
        return max(1, math.ceil(row_count / rows_per_page))
    
    def _estimate_avg_row_size(self, table_info: TableInfo) -> float:
        """估算平均行大小"""
        total_size = 0
        for column in table_info.columns:
            if column.data_type.value == 'INT':
                total_size += 4
            elif column.data_type.value == 'VARCHAR':
                total_size += 50  # 假设平均50字符
            elif column.data_type.value == 'DECIMAL':
                total_size += 8
            else:
                total_size += 4
        return total_size
    
    def _estimate_distinct_count(self, table_name: str, column_name: str) -> int:
        """优先使用 catalog 中的真实 distinct，否则估算列的唯一值数量"""
        if hasattr(self, 'catalog') and self.catalog is not None:
            try:
                cs = self.catalog.get_column_stats(table_name, column_name)
                if cs and isinstance(cs.get('distinct'), int) and cs['distinct'] > 0:
                    return cs['distinct']
            except Exception:
                pass
        row_count = self._estimate_row_count(table_name)
        return max(1, row_count // 2)
    
    def _generate_optimization_report(self, original_plan: LogicalPlan, optimized_plan: LogicalPlan):
        """生成优化报告"""
        print("\n📋 优化报告:")
        print("   " + "="*50)
        
        # 计算优化效果
        original_cost = self.cost_model.calculate_cost(original_plan, self.statistics)
        optimized_cost = self.cost_model.calculate_cost(optimized_plan, self.statistics)
        
        improvement = ((original_cost.total_cost - optimized_cost.total_cost) / original_cost.total_cost) * 100
        
        print(f"   原始计划成本: {original_cost.total_cost:.2f}")
        print(f"   优化后成本: {optimized_cost.total_cost:.2f}")
        print(f"   性能提升: {improvement:.1f}%")
        
        # 成本分解
        print(f"\n   📊 成本分解:")
        print(f"     I/O成本: {optimized_cost.io_cost:.2f}")
        print(f"     CPU成本: {optimized_cost.cpu_cost:.2f}")
        print(f"     内存成本: {optimized_cost.memory_cost:.2f}")

class CostModel:
    """成本模型 - 支持基于真实统计数据的动态成本计算"""
    
    def __init__(self, catalog_manager=None, adaptive_params=None):
        # 基础成本参数（可根据硬件和系统负载动态调整）
        self.io_cost_per_page = 1.0      # 每页I/O成本
        self.cpu_cost_per_row = 0.001    # 每行CPU成本
        self.memory_cost_per_page = 0.1  # 每页内存成本
        
        # 索引相关成本参数
        self.index_seek_cost_per_level = 0.1  # 每层索引查找成本
        self.index_scan_cost_per_row = 0.01   # 每行索引扫描成本
        self.index_fetch_cost_per_page = 0.1  # 每页回表成本
        self.index_cpu_cost_per_row = 0.0001  # 每行索引CPU成本
        self.index_memory_cost = 0.05         # 索引缓存成本
        
        # 保存catalog引用
        self.catalog = catalog_manager
        
        # 自适应参数
        self.adaptive_params = adaptive_params
        if self.adaptive_params:
            self._apply_adaptive_parameters()
    
    def _apply_adaptive_parameters(self):
        """应用自适应参数"""
        if self.adaptive_params:
            self.io_cost_per_page = self.adaptive_params.io_cost_per_page
            self.cpu_cost_per_row = self.adaptive_params.cpu_cost_per_row
            self.memory_cost_per_page = self.adaptive_params.memory_cost_per_page
            self.index_seek_cost_per_level = self.adaptive_params.index_seek_cost_per_level
            self.index_scan_cost_per_row = self.adaptive_params.index_scan_cost_per_row
            self.index_fetch_cost_per_page = self.adaptive_params.index_fetch_cost_per_page
            self.index_cpu_cost_per_row = self.adaptive_params.index_cpu_cost_per_row
            self.index_memory_cost = self.adaptive_params.index_memory_cost
    
    def calculate_cost(self, plan: LogicalPlan, statistics: Dict[str, Statistics]) -> CostInfo:
        """计算执行计划成本"""
        return self._calculate_operator_cost(plan.root, statistics)
    
    def _calculate_operator_cost(self, operator: LogicalOperator, statistics: Dict[str, Statistics]) -> CostInfo:
        """计算单个操作符成本（递归计算子操作符）"""
        if hasattr(operator, 'operator_type'):
            op_type = operator.operator_type.value
        else:
            op_type = type(operator).__name__
        
        # 计算当前操作符的成本
        if op_type == 'Scan':
            current_cost = self._calculate_scan_cost(operator, statistics)
        elif op_type == 'IndexScan':
            current_cost = self._calculate_index_scan_cost(operator, statistics)
        elif op_type == 'Filter':
            current_cost = self._calculate_filter_cost(operator, statistics)
        elif op_type == 'Project':
            current_cost = self._calculate_project_cost(operator, statistics)
        elif op_type == 'Join':
            current_cost = self._calculate_join_cost(operator, statistics)
        else:
            current_cost = CostInfo(io_cost=1.0, cpu_cost=1.0, memory_cost=0.5)
        
        # 递归计算子操作符的成本
        if hasattr(operator, 'children') and operator.children:
            for child in operator.children:
                child_cost = self._calculate_operator_cost(child, statistics)
                # 累加子操作符的成本
                current_cost.io_cost += child_cost.io_cost
                current_cost.cpu_cost += child_cost.cpu_cost
                current_cost.memory_cost += child_cost.memory_cost
                current_cost.total_cost += child_cost.total_cost
        
        return current_cost
    
    def _calculate_scan_cost(self, operator, statistics: Dict[str, Statistics]) -> CostInfo:
        """计算全表扫描成本 - 基于真实统计数据"""
        table_name = getattr(operator, 'table_name', 'unknown')
        stats = statistics.get(table_name, Statistics(table_name))
        
        # 确保有合理的统计信息
        row_count = max(1, stats.row_count)
        page_count = max(1, stats.page_count)
        
        # I/O成本：全表扫描需要读取所有页（顺序I/O，相对高效）
        io_cost = page_count * self.io_cost_per_page
        
        # CPU成本：需要处理所有行（包括不匹配的行）
        cpu_cost = row_count * self.cpu_cost_per_row
        
        # 内存成本：需要缓存数据页
        memory_cost = page_count * self.memory_cost_per_page
        
        # 全表扫描的固有开销：需要扫描整个表，无论选择性如何
        # 这是全表扫描的主要劣势
        return CostInfo(
            io_cost=io_cost,
            cpu_cost=cpu_cost,
            memory_cost=memory_cost
        )

    def _calculate_index_scan_cost(self, operator, statistics: Dict[str, Statistics]) -> CostInfo:
        """计算索引扫描成本 - 基于真实统计数据和动态参数"""
        table_name = getattr(operator, 'table_name', 'unknown')
        stats = statistics.get(table_name, Statistics(table_name))
        row_count = max(1, stats.row_count)
        page_count = max(1, stats.page_count)
        
        # 优先使用真实的选择性估算
        sel = getattr(operator, 'selectivity', None)
        if sel is None:
            # 根据操作符类型和真实统计数据估算选择性
            predicate = getattr(operator, 'predicate', '') or ''
            column_name = getattr(operator, 'column_name', '')
            
            # 尝试从统计信息获取真实的选择性
            if hasattr(self, 'catalog') and self.catalog:
                try:
                    col_stats = self.catalog.get_column_stats(table_name, column_name)
                    if col_stats and 'distinct' in col_stats:
                        distinct_count = col_stats['distinct']
                        if '=' in predicate or '==' in predicate:
                            # 等值查询：使用真实distinct值
                            sel = 1.0 / max(1, distinct_count)
                        elif any(op in predicate for op in ['<', '>', '<=', '>=']):
                            # 范围查询：基于distinct值估算
                            sel = max(0.01, min(0.5, 1.0 / max(1, distinct_count) * 10))
                        else:
                            sel = 0.5
                except Exception:
                    pass
            
            # 如果无法获取真实统计，使用默认估算
            if sel is None:
                if '=' in predicate or '==' in predicate:
                    distinct_count = max(1, row_count // 10)  # 假设有10%的唯一值
                    sel = 1.0 / distinct_count
                elif any(op in predicate for op in ['<', '>', '<=', '>=']):
                    sel = 0.1  # 范围查询，中等选择性
                else:
                    sel = 0.5  # 其他情况，低选择性
        
        matched = max(1.0, row_count * sel)
        # 增加调试日志验证选择性参数
        print(f"[DEBUG] 索引{operator.index_name if hasattr(operator,'index_name') else ''} 选择性: {sel:.4f} 匹配行数: {matched}")
        
        # 改进的索引扫描成本模型
        # 1. 索引查找成本：B+树高度 * 单次I/O成本（索引查找通常很快）
        index_height = max(1, math.ceil(math.log2(max(2, row_count))))
        seek_cost = index_height * self.index_seek_cost_per_level * 0.5  # 索引查找优化
        
        # 2. 索引扫描成本：匹配的索引项数量（索引扫描比全表扫描快）
        scan_cost = matched * self.index_scan_cost_per_row * 0.3  # 索引扫描优化
        
        # 3. 回表成本：从索引获取数据页（这是主要开销）
        pages_to_fetch = max(1, math.ceil(matched / 100))  # 假设每页100行
        fetch_cost = pages_to_fetch * self.index_fetch_cost_per_page
        
        # 4. CPU成本：索引比较和数据处理（索引比较比全表扫描快）
        cpu_cost = matched * self.index_cpu_cost_per_row * 0.2  # CPU优化
        
        # 5. 选择性优化：高选择性时索引优势更明显
        selectivity_factor = 1.0
        if sel < 0.1:  # 高选择性（<10%）
            selectivity_factor = 0.3
        elif sel < 0.3:  # 中等选择性（<30%）
            selectivity_factor = 0.6
        elif sel < 0.7:  # 低选择性（<70%）
            selectivity_factor = 0.8
        # 选择性>=70%时，索引优势不明显，接近全表扫描
        
        io_cost = (seek_cost + scan_cost + fetch_cost) * selectivity_factor
        memory_cost = self.index_memory_cost * 0.5  # 索引内存占用较少
        
        return CostInfo(io_cost=io_cost, cpu_cost=cpu_cost, memory_cost=memory_cost)
    
    def _calculate_filter_cost(self, operator, statistics: Dict[str, Statistics]) -> CostInfo:
        """计算过滤成本"""
        # 过滤成本主要来自CPU
        condition = getattr(operator, 'condition', None)
        if condition:
            # 简化：假设过滤条件复杂度为中等
            cpu_cost = 0.01
        else:
            cpu_cost = 0.001
        
        return CostInfo(io_cost=0.0, cpu_cost=cpu_cost, memory_cost=0.05)
    
    def _calculate_project_cost(self, operator, statistics: Dict[str, Statistics]) -> CostInfo:
        """计算投影成本"""
        # 投影成本较低
        columns = getattr(operator, 'columns', [])
        cpu_cost = len(columns) * 0.001
        
        return CostInfo(io_cost=0.0, cpu_cost=cpu_cost, memory_cost=0.02)
    
    def _calculate_join_cost(self, operator, statistics: Dict[str, Statistics]) -> CostInfo:
        """计算连接成本 - 支持不同连接方法"""
        join_method = getattr(operator, 'join_method', JoinMethod.NESTED_LOOP)
        
        # 获取左右子表的统计信息
        left_stats = None
        right_stats = None
        
        if hasattr(operator, 'left_child') and operator.left_child:
            left_table_name = getattr(operator.left_child, 'table_name', None)
            if left_table_name:
                left_stats = statistics.get(left_table_name)
        
        if hasattr(operator, 'right_child') and operator.right_child:
            right_table_name = getattr(operator.right_child, 'table_name', None)
            if right_table_name:
                right_stats = statistics.get(right_table_name)
        
        if not left_stats or not right_stats:
            # 回退到默认成本
            return CostInfo(io_cost=0.0, cpu_cost=0.1, memory_cost=0.2)
        
        left_rows = left_stats.row_count
        right_rows = right_stats.row_count
        
        if join_method == JoinMethod.NESTED_LOOP:
            # 嵌套循环连接：O(M * N)
            cpu_cost = left_rows * right_rows * 0.0001
            memory_cost = 0.1  # 只需要少量内存
            io_cost = 0.0  # 假设数据在内存中
            
        elif join_method == JoinMethod.HASH_JOIN:
            # 哈希连接：O(M + N)
            # 构建阶段：扫描小表，构建哈希表
            build_cost = min(left_rows, right_rows) * 0.001
            # 探测阶段：扫描大表，查找哈希表
            probe_cost = max(left_rows, right_rows) * 0.0005
            cpu_cost = build_cost + probe_cost
            # 内存成本：哈希表大小
            memory_cost = min(left_rows, right_rows) * 0.01
            io_cost = 0.0
            
        elif join_method == JoinMethod.SORT_MERGE:
            # 排序合并连接：O(M log M + N log N + M + N)
            left_sort_cost = left_rows * math.log2(max(1, left_rows)) * 0.0001
            right_sort_cost = right_rows * math.log2(max(1, right_rows)) * 0.0001
            merge_cost = (left_rows + right_rows) * 0.0001
            cpu_cost = left_sort_cost + right_sort_cost + merge_cost
            # 排序需要临时存储空间
            memory_cost = (left_rows + right_rows) * 0.005
            io_cost = 0.0
            
        else:
            # 默认成本
            cpu_cost = 0.1
            memory_cost = 0.2
            io_cost = 0.0
        
        return CostInfo(io_cost=io_cost, cpu_cost=cpu_cost, memory_cost=memory_cost)
    
    def _get_table_stats_from_operator(self, operator) -> Optional[Statistics]:
        """从操作符获取表统计信息"""
        if hasattr(operator, 'table_name'):
            table_name = operator.table_name
            # 这里需要从statistics参数中获取，但当前方法签名不包含statistics
            # 简化实现：返回None
            return None
        return None

class RuleEngine:
    """规则引擎"""
    
    def __init__(self, catalog_manager: CatalogManager | None = None, statistics_supplier=None):
        self.rules = [
            self._rule_use_index_if_available,
            self._rule_optimize_order_by_with_index,
            self._rule_push_predicates_down,
            self._rule_eliminate_redundant_operations,
            self._rule_optimize_join_order,
            self._rule_constant_folding,
            self._rule_subquery_optimization,
            self._rule_predicate_simplification,
            self._rule_column_elimination
        ]
        self.catalog = catalog_manager
        # 简化的范围选择阈值（命中行比例 < 0.1 则倾向索引）
        self.range_selectivity_threshold = 0.1
        self._statistics_supplier = statistics_supplier or (lambda: {})
        # 决策记录用于可视化
        self.decisions: List[Dict[str, Any]] = []
    
    def optimize(self, plan: LogicalPlan) -> LogicalPlan:
        """应用优化规则"""
        optimized_plan = plan
        
        for rule in self.rules:
            optimized_plan = rule(optimized_plan)
        
        return optimized_plan
    
    def _rule_use_index_if_available(self, plan: LogicalPlan) -> LogicalPlan:
        """规则：如果 WHERE 条件列存在索引，将 Scan+Filter 改为 IndexScan。
        支持复杂结构：LIMIT(ORDER_BY(PROJECT(FILTER(SCAN))))等
        """
        # 递归查找并替换Filter(Scan)模式，保留其他算子
        self._replace_filter_scan_with_index_scan(plan.root, plan)
        return plan
    
    def _find_parent_of_node(self, root, target_node):
        """查找目标节点的父节点"""
        if not hasattr(root, 'children') or not root.children:
            return None
        
        for child in root.children:
            if child is target_node:
                return root
            parent = self._find_parent_of_node(child, target_node)
            if parent:
                return parent
        return None
    
    def _rule_optimize_order_by_with_index(self, plan: LogicalPlan) -> LogicalPlan:
        """规则：对于ORDER BY查询，如果ORDER BY的列有索引，使用索引扫描代替全表扫描+排序"""
        from .logical_operators import IndexScanOperator
        
        # 查找ORDER BY算子
        order_by_node = self._find_order_by_node(plan.root)
        if not order_by_node:
            print("[DEBUG] 没有找到ORDER BY算子")
            return plan
        
        # 检查ORDER BY下是否有SCAN算子
        scan_node = self._find_scan_under_order_by(order_by_node)
        if not scan_node:
            return plan
        
        # 获取ORDER BY的列信息
        if not hasattr(order_by_node, 'order_items') or not order_by_node.order_items:
            return plan
        
        # 目前只优化单列ORDER BY
        if len(order_by_node.order_items) != 1:
            return plan
        
        order_item = order_by_node.order_items[0]
        order_column = order_item['column']
        order_direction = order_item.get('direction', 'ASC')
        
        # 检查该列是否有索引
        table_name = scan_node.table_name
        if not self.catalog:
            return plan
        
        table_info = self.catalog.get_table(table_name)
        if not table_info or not table_info.indexes:
            return plan
        
        # 查找匹配的索引
        matching_index = None
        for index_name, index_info in table_info.indexes.items():
            if isinstance(index_info, dict):
                index_columns = (index_info.get('columns') or 
                               index_info.get('column_names') or 
                               [index_name])
            else:
                index_columns = (getattr(index_info, 'columns', None) or 
                               getattr(index_info, 'column_names', None) or
                               [index_name])
            
            # 检查索引是否包含ORDER BY的列
            if index_columns and index_columns[0] == order_column:
                matching_index = index_name
                break
        
        if not matching_index:
            return plan
        
        # 创建IndexScan来替换Scan，并移除OrderBy算子
        idx_scan = IndexScanOperator(
            table_name=table_name,
            index_name=matching_index,
            column_name=order_column,
            predicate=None  # 没有WHERE条件
        )
        
        # 设置索引扫描的排序方向
        setattr(idx_scan, 'order_direction', order_direction)
        
        # 替换算子树结构：保留PROJECT等其他算子，只替换ORDER BY -> SCAN为IndexScan
        # 找到ORDER BY的子算子（通常是PROJECT）
        order_by_children = []
        if hasattr(order_by_node, 'children') and order_by_node.children:
            for child in order_by_node.children:
                if child is not scan_node:  # 保留非SCAN的子算子
                    order_by_children.append(child)
        
        # 将保留的子算子连接到IndexScan
        for child in order_by_children:
            # 在子算子中查找并替换SCAN
            self._replace_scan_with_index_scan(child, scan_node, idx_scan)
        
        # 替换ORDER BY节点
        parent = self._find_parent_of_node(plan.root, order_by_node)
        if parent:
            # ORDER BY有父节点
            for i, child in enumerate(parent.children):
                if child is order_by_node:
                    if order_by_children:
                        # 用第一个子算子替换ORDER BY
                        parent.children[i] = order_by_children[0]
                    else:
                        # 没有子算子，直接用IndexScan替换
                        parent.children[i] = idx_scan
                    break
        else:
            # ORDER BY是根节点
            if order_by_children:
                plan.root = order_by_children[0]
            else:
                plan.root = idx_scan
        
        self.decisions.append({
            'table': table_name,
            'column': order_column,
            'optimization': 'ORDER_BY_INDEX',
            'index_name': matching_index,
            'direction': order_direction,
            'chosen': True,
            'selectivity_estimate': 1.0  # ORDER BY索引优化不涉及选择性
        })
        
        return plan
    
    def _find_order_by_node(self, node):
        """查找ORDER BY算子"""
        from .logical_operators import OrderByOperator
        if isinstance(node, OrderByOperator):
            return node
        
        if hasattr(node, 'children') and node.children:
            for child in node.children:
                result = self._find_order_by_node(child)
                if result:
                    return result
        return None
    
    def _find_scan_under_order_by(self, order_by_node):
        """查找ORDER BY下的SCAN算子"""
        from .logical_operators import ScanOperator
        
        def find_scan(node):
            if isinstance(node, ScanOperator):
                return node
            if hasattr(node, 'children') and node.children:
                for child in node.children:
                    result = find_scan(child)
                    if result:
                        return result
            return None
        
        return find_scan(order_by_node)
    
    def _replace_scan_with_index_scan(self, node, old_scan, new_index_scan):
        """在算子树中查找并替换指定的SCAN节点为IndexScan"""
        if hasattr(node, 'children') and node.children:
            for i, child in enumerate(node.children):
                if child is old_scan:
                    node.children[i] = new_index_scan
                else:
                    self._replace_scan_with_index_scan(child, old_scan, new_index_scan)
    
    def _replace_filter_scan_with_index_scan(self, node, plan):
        """
        【修复版】递归查找并替换 Filter(Scan) 为 IndexScan。
        修复了对AST条件节点的解析逻辑。
        """
        # 递归处理子节点
        if hasattr(node, 'children') and node.children:
            # 使用 list() 创建副本进行迭代，因为我们可能会修改列表
            for child in list(node.children):
                self._replace_filter_scan_with_index_scan(child, plan)
        
        # 检查当前节点是否是 Filter(Scan) 模式
        if isinstance(node, FilterOperator) and node.children:
            child = node.children[0]
            if isinstance(child, ScanOperator):
                table_name = child.table_name
                
                # --- 【核心修复】使用新的解析函数 ---
                expr = getattr(node, 'condition', None)
                column_name, value, op = self.extract_eq_column_value(expr)
                
                # 如果没有解析出 "列 = 值" 的简单条件，则跳过此优化
                if not (column_name and value is not None and op in ('=', '==')):
                    return

                # --- 核心决策逻辑 ---
                has_index = False
                index_name = None
                
                if self.catalog and self.catalog.has_index_on(table_name, column_name):
                    has_index = True
                    index_name = self.catalog.get_index_by_column(table_name, column_name)

                # 如果有索引，就直接替换！
                if has_index:
                    # 创建新的 IndexScan 逻辑算子
                    # 注意： predicate_key 必须是元组
                    idx_scan = IndexScanOperator(
                        table_name=table_name,
                        index_name=index_name,
                        column_name=column_name,
                        predicate={'key': (value,)}
                    )

                    # 在计划树中替换掉 Filter 节点
                    parent = self._find_parent_of_node(plan.root, node)
                    if parent:
                        for i, child_node in enumerate(parent.children):
                            if child_node is node:
                                parent.children[i] = idx_scan
                                break
                    else: # Filter 是根节点
                        plan.root = idx_scan
                    
                    # 记录决策（用于调试）
                    self.decisions.append({
                        'table': table_name, 'column': column_name,
                        'predicate': str(expr), 'chosen': True, 'reason': '索引存在',
                        'index_name': index_name
                    })

    def extract_eq_column_value(self, expr):
        """
        【二次修复版】解析AST表达式，提取等值条件的列名和值。
        专门用于解析形如 BinaryExpr(left=Identifier, operator=Token, right=Literal) 的AST节点。
        """
        # 检查 expr 是否是 BinaryExpr AST 节点对象
        # 【关键修复】: 属性名是 operator, 而不是 op
        if hasattr(expr, 'operator') and hasattr(expr, 'left') and hasattr(expr, 'right'):
            op_token = getattr(expr, 'operator', None)
            left_node = getattr(expr, 'left', None)
            right_node = getattr(expr, 'right', None)

            # 检查操作符Token是否为等号
            if not (hasattr(op_token, 'literal') and op_token.literal in ('=', '==')):
                return None, None, None

            # 假设左边是列名 (Identifier), 右边是值 (Literal)
            # 它们都有一个 .value 属性
            print(2)
            if hasattr(left_node, 'value') and hasattr(right_node, 'value'):
                print(3)
                column_name = left_node.value
                value = right_node.value
                op = op_token.literal
                
                # 关键：将从 Literal 节点中取出的值（它可能是字符串 '1'）尝试转换为数字
                try:
                    # 尝试转为整数，如果失败再尝试浮点数
                    if isinstance(value, str) and '.' in value:
                        value = float(value)
                    else:
                        value = int(value)
                except (ValueError, TypeError):
                    # 如果转换失败，保持原样（例如字符串类型的值）
                    pass
                
                return column_name, value, op

        # 如果不匹配上面的结构，返回None
        return None, None, None

    def _is_equality_op(self, op: Optional[str]) -> bool:
        return op in ('=', '==')

    def _is_range_op(self, op: Optional[str]) -> bool:
        return op in ('<', '>', '<=', '>=')

    def _estimate_range_selectivity(self, table: str, column: str, cond_str: str) -> float:
        """改进的选择性估算：
        - 等值查询：假设10%选择性
        - 范围查询：假设20%选择性
        - 其他：假设50%选择性
        """
        name = column.lower()
        if 'id' == name or 'time' in name:
            return 0.05  # 5%选择性
        elif '=' in cond_str or '==' in cond_str:
            return 0.1   # 等值查询，10%选择性
        elif any(op in cond_str for op in ['<', '>', '<=', '>=']):
            return 0.2   # 范围查询，20%选择性
        else:
            return 0.5   # 其他情况，50%选择性

    def get_decisions(self) -> List[Dict[str, Any]]:
        return list(self.decisions)
    
    def _rule_push_predicates_down(self, plan: LogicalPlan) -> LogicalPlan:
        """规则：谓词下推"""
        # 将过滤条件尽可能靠近数据源
        if plan.node_type == 'Join' and hasattr(plan.root, 'condition'):
            # 将连接条件下推到子节点
            if hasattr(plan.root, 'left_child') and plan.root.left_child:
                left_plan = LogicalPlan(plan.root.left_child)
                if left_plan.node_type == 'Scan':
                    if not hasattr(plan.root.left_child, 'condition'):
                        plan.root.left_child.condition = []
                    # 提取左表相关的条件
                    left_conditions = self._extract_table_conditions([plan.root.condition], plan.root.left_child.table_name)
                    if left_conditions:
                        plan.root.left_child.condition.extend(left_conditions)
            
            if hasattr(plan.root, 'right_child') and plan.root.right_child:
                right_plan = LogicalPlan(plan.root.right_child)
                if right_plan.node_type == 'Scan':
                    if not hasattr(plan.root.right_child, 'condition'):
                        plan.root.right_child.condition = []
                    # 提取右表相关的条件
                    right_conditions = self._extract_table_conditions([plan.root.condition], plan.root.right_child.table_name)
                    if right_conditions:
                        plan.root.right_child.condition.extend(right_conditions)
        
        # 递归处理子节点
        if hasattr(plan.root, 'left_child') and plan.root.left_child:
            left_plan = LogicalPlan(plan.root.left_child)
            plan.root.left_child = self._rule_push_predicates_down(left_plan).root
        if hasattr(plan.root, 'right_child') and plan.root.right_child:
            right_plan = LogicalPlan(plan.root.right_child)
            plan.root.right_child = self._rule_push_predicates_down(right_plan).root
        
        return plan
    
    def _extract_table_conditions(self, conditions: List, table_name: str) -> List:
        """提取特定表相关的条件"""
        table_conditions = []
        for condition in conditions:
            if hasattr(condition, 'left') and hasattr(condition, 'right'):
                # 检查条件是否只涉及指定表
                if (self._is_table_column(condition.left, table_name) and 
                    self._is_table_column(condition.right, table_name)):
                    table_conditions.append(condition)
        return table_conditions
    
    def _is_table_column(self, expr, table_name: str) -> bool:
        """检查表达式是否只涉及指定表的列"""
        if hasattr(expr, 'table_name'):
            return expr.table_name == table_name
        return False
    
    def _rule_eliminate_redundant_operations(self, plan: LogicalPlan) -> LogicalPlan:
        """规则：消除冗余操作"""
        # 消除重复的投影
        if plan.node_type == 'Project' and hasattr(plan.root, 'children') and plan.root.children:
            left_child = plan.root.children[0]
            left_plan = LogicalPlan(left_child)
            if left_plan.node_type == 'Project':
                # 合并两个投影操作
                if hasattr(plan.root, 'columns') and hasattr(left_child, 'columns'):
                    combined_columns = list(set(plan.root.columns + left_child.columns))
                    plan.root.columns = combined_columns
                    plan.root.children = left_child.children
        
        # 消除重复的过滤
        if plan.node_type == 'Filter' and hasattr(plan.root, 'children') and plan.root.children:
            left_child = plan.root.children[0]
            left_plan = LogicalPlan(left_child)
            if left_plan.node_type == 'Filter':
                # 合并两个过滤条件
                if hasattr(plan.root, 'condition') and hasattr(left_child, 'condition'):
                    # 简化实现：保持原条件
                    pass
        
        # 递归处理子节点
        if hasattr(plan.root, 'children') and plan.root.children:
            for i, child in enumerate(plan.root.children):
                child_plan = LogicalPlan(child)
                plan.root.children[i] = self._rule_eliminate_redundant_operations(child_plan).root
        
        return plan
    
    def _rule_optimize_join_order(self, plan: LogicalPlan) -> LogicalPlan:
        """规则：优化连接顺序"""
        # 基于成本重新排序连接
        if plan.node_type == 'Join':
            # 简化实现：暂时不进行连接顺序优化
            pass
        
        return plan
    
    def _rule_constant_folding(self, plan: LogicalPlan) -> LogicalPlan:
        """规则：常量折叠 - 在编译时计算常量表达式"""
        # 遍历计划树，查找可以折叠的常量表达式
        self._fold_constants_in_operator(plan.root)
        return plan
    
    def _fold_constants_in_operator(self, operator):
        """在操作符中折叠常量"""
        if hasattr(operator, 'condition'):
            operator.condition = self._fold_expression(operator.condition)
        
        # 递归处理子操作符
        if hasattr(operator, 'children') and operator.children:
            for child in operator.children:
                self._fold_constants_in_operator(child)
    
    def _fold_expression(self, expression):
        """折叠表达式中的常量"""
        if hasattr(expression, 'left') and hasattr(expression, 'right'):
            # 二元表达式
            left_folded = self._fold_expression(expression.left)
            right_folded = self._fold_expression(expression.right)
            
            # 如果左右都是字面量，可以折叠
            if (hasattr(left_folded, 'value') and hasattr(right_folded, 'value') and
                hasattr(expression, 'operator')):
                try:
                    result = self._evaluate_binary_expression(
                        left_folded.value, expression.operator, right_folded.value
                    )
                    from .logical_operators import LiteralExpression
                    return LiteralExpression(result)
                except:
                    pass
            
            # 创建新的表达式
            from .logical_operators import BinaryExpression
            new_expr = BinaryExpression(left_folded, expression.operator, right_folded)
            return new_expr
        
        return expression
    
    def _evaluate_binary_expression(self, left, operator, right):
        """计算二元表达式"""
        if operator == '+':
            return left + right
        elif operator == '-':
            return left - right
        elif operator == '*':
            return left * right
        elif operator == '/':
            return left / right if right != 0 else 0
        elif operator == '=' or operator == '==':
            return left == right
        elif operator == '!=' or operator == '<>':
            return left != right
        elif operator == '<':
            return left < right
        elif operator == '>':
            return left > right
        elif operator == '<=':
            return left <= right
        elif operator == '>=':
            return left >= right
        else:
            raise ValueError(f"Unsupported operator: {operator}")
    
    def _rule_subquery_optimization(self, plan: LogicalPlan) -> LogicalPlan:
        """规则：子查询优化 - 将相关子查询转换为连接"""
        # 查找子查询并尝试转换为连接
        self._optimize_subqueries_in_operator(plan.root)
        return plan
    
    def _optimize_subqueries_in_operator(self, operator):
        """在操作符中优化子查询"""
        if hasattr(operator, 'condition'):
            operator.condition = self._optimize_subquery_in_expression(operator.condition)
        
        # 递归处理子操作符
        if hasattr(operator, 'children') and operator.children:
            for child in operator.children:
                self._optimize_subqueries_in_operator(child)
    
    def _optimize_subquery_in_expression(self, expression):
        """在表达式中优化子查询"""
        # 这里可以实现将EXISTS子查询转换为半连接等优化
        # 由于当前系统结构限制，这里先返回原表达式
        return expression
    
    def _rule_predicate_simplification(self, plan: LogicalPlan) -> LogicalPlan:
        """规则：谓词简化 - 简化复杂的谓词条件"""
        self._simplify_predicates_in_operator(plan.root)
        return plan
    
    def _simplify_predicates_in_operator(self, operator):
        """在操作符中简化谓词"""
        if hasattr(operator, 'condition'):
            operator.condition = self._simplify_expression(operator.condition)
        
        # 递归处理子操作符
        if hasattr(operator, 'children') and operator.children:
            for child in operator.children:
                self._simplify_predicates_in_operator(child)
    
    def _simplify_expression(self, expression):
        """简化表达式"""
        if hasattr(expression, 'left') and hasattr(expression, 'right'):
            # 简化二元表达式
            left_simplified = self._simplify_expression(expression.left)
            right_simplified = self._simplify_expression(expression.right)
            
            # 处理一些常见的简化规则
            if (hasattr(expression, 'operator') and 
                hasattr(left_simplified, 'value') and 
                hasattr(right_simplified, 'value')):
                
                # 处理恒真/恒假条件
                if expression.operator in ['=', '=='] and left_simplified.value == right_simplified.value:
                    from .logical_operators import LiteralExpression
                    return LiteralExpression(True)
                elif expression.operator in ['!=', '<>'] and left_simplified.value == right_simplified.value:
                    from .logical_operators import LiteralExpression
                    return LiteralExpression(False)
            
            from .logical_operators import BinaryExpression
            return BinaryExpression(left_simplified, expression.operator, right_simplified)
        
        return expression
    
    def _rule_column_elimination(self, plan: LogicalPlan) -> LogicalPlan:
        """规则：列消除 - 消除不需要的列"""
        # 分析查询中实际使用的列，消除未使用的列
        self._eliminate_unused_columns_in_operator(plan.root)
        return plan
    
    def _eliminate_unused_columns_in_operator(self, operator):
        """在操作符中消除未使用的列"""
        if hasattr(operator, 'columns'):
            # 这里可以实现列使用分析，消除未引用的列
            # 由于需要复杂的依赖分析，这里先保持原样
            pass
        
        # 递归处理子操作符
        if hasattr(operator, 'children') and operator.children:
            for child in operator.children:
                self._eliminate_unused_columns_in_operator(child)
    
    def _estimate_join_cardinality(self, left_card: int, right_card: int, join_type: str) -> int:
        """估算连接基数"""
        if join_type == 'inner':
            # 内连接：假设选择性为0.1
            return int(left_card * right_card * 0.1)
        elif join_type == 'left':
            # 左外连接：至少等于左表基数
            return max(left_card, int(left_card * right_card * 0.1))
        elif join_type == 'right':
            # 右外连接：至少等于右表基数
            return max(right_card, int(left_card * right_card * 0.1))
        else:
            # 默认情况
            return int(left_card * right_card * 0.1)

class OptimizationAnalyzer:
    """优化分析器"""
    
    def __init__(self, optimizer: QueryOptimizer):
        self.optimizer = optimizer
    
    def analyze_query(self, sql: str) -> Dict[str, Any]:
        """分析查询并提供优化建议"""
        # 1. 解析查询
        from .lexicalAnalysis import tokenize
        from .new_syntax_analyzer import NewSyntaxAnalyzer as SyntaxAnalyzer
        from .enhanced_semantic_analyzer import EnhancedSemanticAnalyzer as SemanticAnalyzer
        
        tokens = tokenize(sql)
        syntax_analyzer = SyntaxAnalyzer()
        ast = syntax_analyzer.build_ast_from_tokens(tokens)
        
        semantic_analyzer = SemanticAnalyzer(self.optimizer.symbol_table)
        if not semantic_analyzer.analyze(ast):
            return {"error": "语义分析失败"}
        
        # 2. 生成执行计划
        planner = EnhancedQueryPlanner(self.optimizer.symbol_table)
        logical_plan = planner.create_plan(ast)
        
        # 3. 优化计划
        optimized_plan = self.optimizer.optimize(logical_plan)
        
        # 4. 分析结果
        analysis = {
            "original_plan": self._plan_to_dict(logical_plan),
            "optimized_plan": self._plan_to_dict(optimized_plan),
            "optimization_suggestions": self._generate_suggestions(sql, logical_plan, optimized_plan),
            "performance_metrics": self._calculate_metrics(logical_plan, optimized_plan),
            "index_decisions": self._format_index_decisions()
        }
        
        return analysis

    def _plan_to_dict(self, plan: LogicalPlan) -> Dict[str, Any]:
        """将执行计划转换为字典（用于报告展示）"""
        return {
            "root_operator": type(plan.root).__name__,
            "cost": self.optimizer.cost_model.calculate_cost(plan, self.optimizer.statistics).total_cost
        }

    def _generate_suggestions(self, sql: str, original_plan: LogicalPlan, optimized_plan: LogicalPlan) -> List[str]:
        """生成优化建议"""
        suggestions: List[str] = []
        if "WHERE" in sql.upper() and not self._has_index_usage(original_plan):
            suggestions.append("考虑为WHERE条件中的列创建索引")
        if "SELECT *" in sql.upper():
            suggestions.append("避免使用SELECT *，只选择需要的列")
        if "JOIN" in sql.upper():
            suggestions.append("检查表连接顺序，将小表放在前面")
        return suggestions

    def _has_index_usage(self, plan: LogicalPlan) -> bool:
        """检查是否使用了索引（简化实现，占位）"""
        return False

    def _calculate_metrics(self, original_plan: LogicalPlan, optimized_plan: LogicalPlan) -> Dict[str, Any]:
        """计算性能指标"""
        original_cost = self.optimizer.cost_model.calculate_cost(original_plan, self.optimizer.statistics)
        optimized_cost = self.optimizer.cost_model.calculate_cost(optimized_plan, self.optimizer.statistics)
        improvement = ((original_cost.total_cost - optimized_cost.total_cost) / original_cost.total_cost) * 100 if original_cost.total_cost > 0 else 0.0
        io_reduction = ((original_cost.io_cost - optimized_cost.io_cost) / original_cost.io_cost) * 100 if original_cost.io_cost > 0 else 0.0
        cpu_reduction = ((original_cost.cpu_cost - optimized_cost.cpu_cost) / original_cost.cpu_cost) * 100 if original_cost.cpu_cost > 0 else 0.0
        return {
            "original_cost": original_cost.total_cost,
            "optimized_cost": optimized_cost.total_cost,
            "improvement_percentage": improvement,
            "io_reduction": io_reduction,
            "cpu_reduction": cpu_reduction
        }

    def _format_index_decisions(self) -> List[Dict[str, Any]]:
        """返回索引选择可视化数据"""
        decisions = []
        if hasattr(self.optimizer.rule_engine, 'get_decisions'):
            for d in self.optimizer.rule_engine.get_decisions():
                # 生成简洁条目
                decisions.append({
                    'table': d.get('table'),
                    'column': d.get('column'),
                    'predicate': d.get('predicate'),
                    'operator': d.get('operator'),
                    'selectivity': round(float(d.get('selectivity_estimate', 0.0)), 4),
                    'seq_cost': round(float(d.get('seq_cost_estimate', 0.0)), 4),
                    'index_cost': round(float(d.get('index_cost_estimate', 0.0)), 4),
                    'chosen': d.get('chosen'),
                    'index_name': d.get('index_name')
                })
        return decisions


class CardinalityEstimator:
    """基数/选择性估算器：使用 CatalogManager 中的列统计"""
    def __init__(self, catalog: CatalogManager | None):
        self.catalog = catalog

    def estimate_selectivity(self, table: str, column: str, op: Optional[str], cond_str: str) -> Optional[float]:
        if self.catalog is None:
            return None
        try:
            cs = self.catalog.get_column_stats(table, column)
            t = self.catalog.get_table(table)
        except Exception:
            return None
        if cs is None:
            return None
        row_count = max(1, getattr(t, 'row_count', 0))
        distinct = max(1, int(cs.get('distinct') or 1))
        # 尝试解析右值
        rhs_value = self._parse_rhs_value(cond_str)
        # 等值：优先用 MCV
        if op in ('=', '=='):
            mcv = cs.get('mcv') or []  # [(value, freq)]
            if rhs_value is not None and mcv:
                total = max(1, getattr(t, 'row_count', 1))
                for val, freq in mcv:
                    if str(val) == str(rhs_value):
                        return min(1.0, float(freq) / float(total))
            return min(1.0, 1.0 / distinct)
        if op in ('<', '<=', '>', '>='):
            # 先尝试直方图
            hist = cs.get('histogram') or []  # [(bucket_high, freq)] 累计或单桶频次
            total_rows = max(1, getattr(t, 'row_count', 1))
            if rhs_value is not None and hist:
                try:
                    # 视为按 bucket_high 升序
                    buckets = sorted(hist, key=lambda x: float(x[0]))
                    cum = 0.0
                    if op in ('<', '<='):
                        for bh, freq in buckets:
                            if float(rhs_value) >= float(bh):
                                cum += float(freq)
                            else:
                                break
                        return max(0.0, min(1.0, cum / float(total_rows)))
                    else:
                        # > or >=
                        for bh, freq in buckets:
                            if float(rhs_value) < float(bh):
                                # 还未覆盖到 rhs_value 的桶都计入右侧剩余
                                pass
                        # 简化：1 - P(x <= v)
                        left = 0.0
                        for bh, freq in buckets:
                            if float(rhs_value) >= float(bh):
                                left += float(freq)
                            else:
                                break
                        return max(0.0, min(1.0, (float(total_rows) - left) / float(total_rows)))
                except Exception:
                    pass
            # 回退用 min/max 线性估计
            cmin = cs.get('min')
            cmax = cs.get('max')
            v = None
            if isinstance(rhs_value, (int, float)):
                v = float(rhs_value)
            if v is not None and isinstance(cmin, (int, float)) and isinstance(cmax, (int, float)) and cmax > cmin:
                if op in ('<', '<='):
                    frac = max(0.0, min(1.0, (v - cmin) / (cmax - cmin)))
                else:
                    frac = max(0.0, min(1.0, (cmax - v) / (cmax - cmin)))
                return frac
            return 0.1
        return None

    def _parse_rhs_value(self, cond_str: str) -> Optional[Any]:
        """从形如"(col OP value)"的字符串中解析右值。"""
        try:
            tokens = cond_str.strip().strip('()').split()
            if len(tokens) < 3:
                return None
            raw = tokens[2]
            s = raw.strip()
            if (s.startswith("'") and s.endswith("'")) or (s.startswith('"') and s.endswith('"')):
                return s[1:-1]
            # 数字
            if '.' in s:
                return float(s)
            return int(s)
        except Exception:
            return None
    
    def _plan_to_dict(self, plan: LogicalPlan) -> Dict[str, Any]:
        """将执行计划转换为字典"""
        return {
            "root_operator": type(plan.root).__name__,
            "cost": self.optimizer.cost_model.calculate_cost(plan, self.optimizer.statistics).total_cost
        }
    
    def _generate_suggestions(self, sql: str, original_plan: LogicalPlan, optimized_plan: LogicalPlan) -> List[str]:
        """生成优化建议"""
        suggestions = []
        
        # 检查是否使用了索引
        if "WHERE" in sql.upper() and not self._has_index_usage(original_plan):
            suggestions.append("考虑为WHERE条件中的列创建索引")
        
        # 检查SELECT *
        if "SELECT *" in sql.upper():
            suggestions.append("避免使用SELECT *，只选择需要的列")
        
        # 检查连接顺序
        if "JOIN" in sql.upper():
            suggestions.append("检查表连接顺序，将小表放在前面")
        
        return suggestions
    
    def _has_index_usage(self, plan: LogicalPlan) -> bool:
        """检查是否使用了索引"""
        # 简化实现
        return False
    
    def _calculate_metrics(self, original_plan: LogicalPlan, optimized_plan: LogicalPlan) -> Dict[str, Any]:
        """计算性能指标"""
        original_cost = self.optimizer.cost_model.calculate_cost(original_plan, self.optimizer.statistics)
        optimized_cost = self.optimizer.cost_model.calculate_cost(optimized_plan, self.optimizer.statistics)
        
        # 避免除零错误
        if original_cost.total_cost > 0:
            improvement = ((original_cost.total_cost - optimized_cost.total_cost) / original_cost.total_cost) * 100
        else:
            improvement = 0.0
        
        # 计算I/O减少百分比
        if original_cost.io_cost > 0:
            io_reduction = ((original_cost.io_cost - optimized_cost.io_cost) / original_cost.io_cost) * 100
        else:
            io_reduction = 0.0
        
        # 计算CPU减少百分比
        if original_cost.cpu_cost > 0:
            cpu_reduction = ((original_cost.cpu_cost - optimized_cost.cpu_cost) / original_cost.cpu_cost) * 100
        else:
            cpu_reduction = 0.0
        
        return {
            "original_cost": original_cost.total_cost,
            "optimized_cost": optimized_cost.total_cost,
            "improvement_percentage": improvement,
            "io_reduction": io_reduction,
            "cpu_reduction": cpu_reduction
        }

def create_optimizer(symbol_table: SymbolTable, strategy: OptimizationStrategy = OptimizationStrategy.HYBRID, catalog_manager: CatalogManager | None = None, enable_optimization: bool = True) -> QueryOptimizer:
    """创建查询优化器实例，允许注入 CatalogManager"""
    return QueryOptimizer(symbol_table, strategy, catalog_manager, enable_optimization)

def optimize_query(sql: str, symbol_table: SymbolTable, catalog_manager: CatalogManager | None = None) -> Dict[str, Any]:
    """优化查询的便捷函数，允许注入 CatalogManager"""
    optimizer = create_optimizer(symbol_table, catalog_manager=catalog_manager)
    analyzer = OptimizationAnalyzer(optimizer)
    return analyzer.analyze_query(sql)
