"""
Performance monitoring utilities for the image viewer.

This module provides tools to monitor and report on performance metrics,
cache hit rates, and optimization effectiveness.
"""

import functools
import time
from typing import Dict, Any, Callable

from qp2.log.logging_config import get_logger

logger = get_logger(__name__)


class PerformanceMonitor:
    """Monitor and track performance metrics for the image viewer."""

    def __init__(self):
        self.timing_data = {}
        self.call_counts = {}
        self.total_times = {}

    def time_function(self, func_name: str = None):
        """Decorator to time function execution."""

        def decorator(func: Callable) -> Callable:
            name = func_name or f"{func.__module__}.{func.__name__}"

            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                start_time = time.perf_counter()
                try:
                    result = func(*args, **kwargs)
                    return result
                finally:
                    end_time = time.perf_counter()
                    execution_time = end_time - start_time

                    # Update statistics
                    if name not in self.timing_data:
                        self.timing_data[name] = []
                        self.call_counts[name] = 0
                        self.total_times[name] = 0.0

                    self.timing_data[name].append(execution_time)
                    self.call_counts[name] += 1
                    self.total_times[name] += execution_time

                    # Keep only last 100 measurements to prevent memory growth
                    if len(self.timing_data[name]) > 100:
                        removed_time = self.timing_data[name].pop(0)
                        self.total_times[name] -= removed_time

                    # Log slow operations
                    if execution_time > 1.0:  # Log operations taking > 1 second
                        logger.warning(f"Slow operation: {name} took {execution_time:.3f}s")
                    elif execution_time > 0.1:  # Debug log for > 100ms
                        logger.debug(f"Operation timing: {name} took {execution_time:.3f}s")

            return wrapper

        return decorator

    def get_performance_report(self) -> Dict[str, Any]:
        """Generate a comprehensive performance report."""
        report = {
            "function_timings": {},
            "cache_statistics": {},
            "summary": {}
        }

        # Function timing statistics
        for func_name in self.timing_data:
            timings = self.timing_data[func_name]
            if timings:
                report["function_timings"][func_name] = {
                    "call_count": self.call_counts[func_name],
                    "total_time": self.total_times[func_name],
                    "average_time": self.total_times[func_name] / len(timings),
                    "min_time": min(timings),
                    "max_time": max(timings),
                    "recent_average": sum(timings[-10:]) / min(10, len(timings))
                }

        # Cache statistics
        try:
            from .performance_cache import get_performance_cache
            cache = get_performance_cache()
            report["cache_statistics"] = cache.get_statistics()
        except Exception as e:
            logger.warning(f"Could not get cache statistics: {e}")
            report["cache_statistics"] = {"error": str(e)}

        # Summary statistics
        total_calls = sum(self.call_counts.values())
        total_time = sum(self.total_times.values())

        report["summary"] = {
            "total_function_calls": total_calls,
            "total_execution_time": total_time,
            "average_call_time": total_time / total_calls if total_calls > 0 else 0.0,
            "monitored_functions": len(self.timing_data)
        }

        return report

    def log_performance_summary(self):
        """Log a summary of performance metrics."""
        report = self.get_performance_report()

        logger.info("=== Performance Summary ===")
        summary = report["summary"]
        logger.info(f"Total function calls: {summary['total_function_calls']}")
        logger.info(f"Total execution time: {summary['total_execution_time']:.3f}s")
        logger.info(f"Average call time: {summary['average_call_time']:.3f}s")
        logger.info(f"Monitored functions: {summary['monitored_functions']}")

        # Log slowest functions
        timings = report["function_timings"]
        if timings:
            slowest = sorted(timings.items(), key=lambda x: x[1]["total_time"], reverse=True)[:5]
            logger.info("Slowest functions (by total time):")
            for func_name, stats in slowest:
                logger.info(f"  {func_name}: {stats['total_time']:.3f}s total, "
                            f"{stats['average_time']:.3f}s avg, {stats['call_count']} calls")

        # Log cache performance
        cache_stats = report["cache_statistics"]
        if "error" not in cache_stats:
            logger.info("Cache Performance:")
            for cache_type, stats in cache_stats.items():
                if isinstance(stats, dict) and "hit_rate" in stats:
                    logger.info(f"  {cache_type}: {stats['hit_rate']:.1f}% hit rate "
                                f"({stats['hits']} hits, {stats['misses']} misses)")

    def clear_statistics(self):
        """Clear all collected performance statistics."""
        self.timing_data.clear()
        self.call_counts.clear()
        self.total_times.clear()
        logger.info("Performance statistics cleared")


# Global performance monitor instance
_global_monitor = None


def get_performance_monitor() -> PerformanceMonitor:
    """Get the global performance monitor instance."""
    global _global_monitor
    if _global_monitor is None:
        _global_monitor = PerformanceMonitor()
    return _global_monitor


def monitor_performance(func_name: str = None):
    """Decorator to monitor function performance."""
    monitor = get_performance_monitor()
    return monitor.time_function(func_name)


def log_cache_performance():
    """Log current cache performance statistics."""
    try:
        from .performance_cache import get_performance_cache
        cache = get_performance_cache()
        stats = cache.get_statistics()

        logger.info("=== Cache Performance ===")
        for cache_type, cache_stats in stats.items():
            if isinstance(cache_stats, dict) and "hit_rate" in cache_stats:
                logger.info(f"{cache_type.title()} Cache: {cache_stats['hit_rate']:.1f}% hit rate "
                            f"({cache_stats['hits']} hits, {cache_stats['misses']} misses, "
                            f"{cache_stats['cache_size']} items)")
    except Exception as e:
        logger.warning(f"Could not log cache performance: {e}")


def optimize_thread_pool():
    """Optimize QThreadPool settings for better performance."""
    try:
        import os
        from PyQt5.QtCore import QThreadPool

        # Set optimal thread count
        cpu_count = os.cpu_count() or 4
        optimal_threads = min(8, max(2, cpu_count))

        thread_pool = QThreadPool.globalInstance()
        thread_pool.setMaxThreadCount(optimal_threads)

        logger.info(f"Optimized thread pool: {optimal_threads} threads (CPU count: {cpu_count})")

    except Exception as e:
        logger.warning(f"Could not optimize thread pool: {e}")


def clear_all_caches():
    """Clear all performance caches."""
    try:
        from .performance_cache import get_performance_cache
        cache = get_performance_cache()
        cache.clear_all()

        monitor = get_performance_monitor()
        monitor.clear_statistics()

        logger.info("All performance caches and statistics cleared")

    except Exception as e:
        logger.warning(f"Could not clear caches: {e}")
