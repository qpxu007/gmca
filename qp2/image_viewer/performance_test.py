#!/usr/bin/env python3
"""
Performance testing script for the optimized image viewer.

This script tests the performance improvements and provides benchmarks
for the various optimizations implemented.
"""

import os
import sys
import time

import numpy as np

# Add the parent directory to the path so we can import qp2
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from qp2.log.logging_config import setup_logging, get_logger
from qp2.image_viewer.utils.performance_cache import get_performance_cache
from qp2.image_viewer.utils.performance_monitor import get_performance_monitor, monitor_performance

# Setup logging
setup_logging(root_name="qp2", log_level="INFO")
logger = get_logger(__name__)


def create_test_image(size: tuple = (2048, 2048), noise_level: float = 10.0) -> np.ndarray:
    """Create a synthetic test image with realistic detector data."""
    h, w = size
    
    # Create base image with Poisson noise
    base_intensity = 100
    image = np.random.poisson(base_intensity, size).astype(np.float32)
    
    # Add some peaks (simulate diffraction spots)
    num_peaks = 50
    for _ in range(num_peaks):
        x = np.random.randint(w//4, 3*w//4)
        y = np.random.randint(h//4, 3*h//4)
        intensity = np.random.uniform(500, 2000)
        sigma = np.random.uniform(2, 5)
        
        # Create Gaussian peak
        yy, xx = np.ogrid[:h, :w]
        peak = intensity * np.exp(-((xx - x)**2 + (yy - y)**2) / (2 * sigma**2))
        image += peak
    
    # Add detector artifacts
    # Saturated pixels
    saturated_mask = np.random.random(size) < 0.001  # 0.1% saturated
    image[saturated_mask] = 65535
    
    # Dead pixels
    dead_mask = np.random.random(size) < 0.0005  # 0.05% dead
    image[dead_mask] = 0
    
    return image


def create_test_mask(size: tuple = (2048, 2048)) -> np.ndarray:
    """Create a test detector mask."""
    h, w = size
    mask = np.zeros(size, dtype=bool)
    
    # Mask beam stop (center circle)
    center_x, center_y = w // 2, h // 2
    yy, xx = np.ogrid[:h, :w]
    center_mask = (xx - center_x)**2 + (yy - center_y)**2 <= 50**2
    mask |= center_mask
    
    # Mask detector gaps
    mask[h//2-5:h//2+5, :] = True  # Horizontal gap
    mask[:, w//2-5:w//2+5] = True  # Vertical gap
    
    return mask


@monitor_performance("contrast_calculation_test")
def test_contrast_calculation_performance():
    """Test contrast calculation performance."""
    logger.info("Testing contrast calculation performance...")
    
    # Test different image sizes
    sizes = [(512, 512), (1024, 1024), (2048, 2048), (4096, 4096)]
    results = {}
    
    for size in sizes:
        logger.info(f"Testing contrast calculation for {size[0]}x{size[1]} image...")
        
        # Create test data
        image = create_test_image(size)
        mask = create_test_mask(size)
        
        # Test original vs optimized
        from qp2.image_viewer.utils.contrast_utils import calculate_contrast_levels
        
        # Warm up cache
        calculate_contrast_levels(image, 1.0, 99.0, mask)
        
        # Time multiple runs
        num_runs = 5
        times = []
        
        for _ in range(num_runs):
            start_time = time.perf_counter()
            vmin, vmax = calculate_contrast_levels(image, 1.0, 99.0, mask)
            end_time = time.perf_counter()
            times.append(end_time - start_time)
        
        avg_time = sum(times) / len(times)
        results[size] = {
            "average_time": avg_time,
            "min_time": min(times),
            "max_time": max(times),
            "contrast_range": (vmin, vmax)
        }
        
        logger.info(f"  Average time: {avg_time:.3f}s (range: {min(times):.3f}-{max(times):.3f}s)")
    
    return results


@monitor_performance("radial_sum_test")
def test_radial_sum_performance():
    """Test radial sum calculation performance."""
    logger.info("Testing radial sum performance...")
    
    from qp2.image_viewer.utils.radial_utils import calculate_radial_statistics_optimized
    
    # Test different image sizes
    sizes = [(512, 512), (1024, 1024), (2048, 2048)]
    results = {}
    
    for size in sizes:
        logger.info(f"Testing radial sum for {size[0]}x{size[1]} image...")
        
        image = create_test_image(size)
        mask = create_test_mask(size)
        center = (size[1] // 2, size[0] // 2)
        max_radius = min(size) // 3
        
        # Time multiple runs
        num_runs = 3
        times = []
        
        for _ in range(num_runs):
            start_time = time.perf_counter()
            stats = calculate_radial_statistics_optimized(image, center, max_radius, mask)
            end_time = time.perf_counter()
            times.append(end_time - start_time)
        
        avg_time = sum(times) / len(times)
        results[size] = {
            "average_time": avg_time,
            "min_time": min(times),
            "max_time": max(times),
            "total_intensity": stats["total_intensity"]
        }
        
        logger.info(f"  Average time: {avg_time:.3f}s (range: {min(times):.3f}-{max(times):.3f}s)")
    
    return results


@monitor_performance("peak_finding_test")
def test_peak_finding_performance():
    """Test peak finding performance."""
    logger.info("Testing peak finding performance...")
    
    from qp2.image_viewer.plugins.spot_finder.peak_finding_utils import find_peaks_in_annulus as find_peaks_in_annulus_optimized
    
    # Create test image with known peaks
    size = (1024, 1024)
    image = create_test_image(size, noise_level=5.0)
    mask = create_test_mask(size)
    center = (size[1] // 2, size[0] // 2)
    
    # Test different annular regions
    test_regions = [
        (50, 200),   # Inner region
        (200, 400),  # Middle region
        (400, 500),  # Outer region
    ]
    
    results = {}
    
    for r1, r2 in test_regions:
        logger.info(f"Testing peak finding in annulus r={r1}-{r2}...")
        
        # Time multiple runs
        num_runs = 3
        times = []
        peak_counts = []
        
        for _ in range(num_runs):
            start_time = time.perf_counter()
            peaks = find_peaks_in_annulus_optimized(
                image, mask, center[0], center[1], r1, r2,
                min_distance=5, num_peaks=100, threshold_abs=200
            )
            end_time = time.perf_counter()
            times.append(end_time - start_time)
            peak_counts.append(len(peaks))
        
        avg_time = sum(times) / len(times)
        avg_peaks = sum(peak_counts) / len(peak_counts)
        
        results[(r1, r2)] = {
            "average_time": avg_time,
            "min_time": min(times),
            "max_time": max(times),
            "average_peaks_found": avg_peaks
        }
        
        logger.info(f"  Average time: {avg_time:.3f}s, peaks found: {avg_peaks:.1f}")
    
    return results


def test_cache_effectiveness():
    """Test cache hit rates and effectiveness."""
    logger.info("Testing cache effectiveness...")
    
    cache = get_performance_cache()
    cache.clear_all()  # Start fresh
    
    # Create test data
    image = create_test_image((1024, 1024))
    mask = create_test_mask((1024, 1024))
    center = (512, 512)
    
    from qp2.image_viewer.utils.contrast_utils import calculate_contrast_levels
    from qp2.image_viewer.utils.radial_utils import calculate_radial_statistics_optimized
    
    # Test contrast caching
    logger.info("Testing contrast calculation caching...")
    
    # First call - should be cache miss
    start_time = time.perf_counter()
    vmin1, vmax1 = calculate_contrast_levels(image, 1.0, 99.0, mask)
    first_time = time.perf_counter() - start_time
    
    # Second call - should be cache hit
    start_time = time.perf_counter()
    vmin2, vmax2 = calculate_contrast_levels(image, 1.0, 99.0, mask)
    second_time = time.perf_counter() - start_time
    
    speedup_contrast = first_time / second_time if second_time > 0 else float('inf')
    logger.info(f"Contrast calculation speedup: {speedup_contrast:.1f}x "
               f"({first_time:.3f}s -> {second_time:.3f}s)")
    
    # Test radial sum caching
    logger.info("Testing radial sum caching...")
    
    # First call - should be cache miss
    start_time = time.perf_counter()
    stats1 = calculate_radial_statistics_optimized(image, center, 400, mask)
    first_time = time.perf_counter() - start_time
    
    # Second call - should be cache hit
    start_time = time.perf_counter()
    stats2 = calculate_radial_statistics_optimized(image, center, 400, mask)
    second_time = time.perf_counter() - start_time
    
    speedup_radial = first_time / second_time if second_time > 0 else float('inf')
    logger.info(f"Radial sum speedup: {speedup_radial:.1f}x "
               f"({first_time:.3f}s -> {second_time:.3f}s)")
    
    # Get cache statistics
    cache_stats = cache.get_statistics()
    logger.info("Cache statistics:")
    for cache_type, stats in cache_stats.items():
        if isinstance(stats, dict) and "hit_rate" in stats:
            logger.info(f"  {cache_type}: {stats['hit_rate']:.1f}% hit rate "
                       f"({stats['hits']} hits, {stats['misses']} misses)")
    
    return {
        "contrast_speedup": speedup_contrast,
        "radial_speedup": speedup_radial,
        "cache_stats": cache_stats
    }


def run_performance_tests():
    """Run all performance tests and generate a report."""
    logger.info("Starting performance tests...")
    
    # Initialize performance monitoring
    monitor = get_performance_monitor()
    
    # Run tests
    contrast_results = test_contrast_calculation_performance()
    radial_results = test_radial_sum_performance()
    peak_results = test_peak_finding_performance()
    cache_results = test_cache_effectiveness()
    
    # Generate performance report
    logger.info("\n" + "="*60)
    logger.info("PERFORMANCE TEST RESULTS")
    logger.info("="*60)
    
    # Contrast calculation results
    logger.info("\nContrast Calculation Performance:")
    for size, stats in contrast_results.items():
        logger.info(f"  {size[0]}x{size[1]}: {stats['average_time']:.3f}s average")
    
    # Radial sum results
    logger.info("\nRadial Sum Performance:")
    for size, stats in radial_results.items():
        logger.info(f"  {size[0]}x{size[1]}: {stats['average_time']:.3f}s average")
    
    # Peak finding results
    logger.info("\nPeak Finding Performance:")
    for region, stats in peak_results.items():
        logger.info(f"  r={region[0]}-{region[1]}: {stats['average_time']:.3f}s average, "
                   f"{stats['average_peaks_found']:.1f} peaks")
    
    # Cache effectiveness
    logger.info("\nCache Effectiveness:")
    logger.info(f"  Contrast calculation speedup: {cache_results['contrast_speedup']:.1f}x")
    logger.info(f"  Radial sum speedup: {cache_results['radial_speedup']:.1f}x")
    
    # Overall performance summary
    monitor.log_performance_summary()
    
    logger.info("\nPerformance tests completed!")
    
    return {
        "contrast": contrast_results,
        "radial": radial_results,
        "peaks": peak_results,
        "cache": cache_results
    }


if __name__ == "__main__":
    try:
        results = run_performance_tests()
        print("\nPerformance tests completed successfully!")
        print("Check the log output for detailed results.")
    except Exception as e:
        logger.error(f"Performance tests failed: {e}", exc_info=True)
        print(f"Performance tests failed: {e}")
        sys.exit(1)