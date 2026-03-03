#!/usr/bin/env python
"""
Test script for the QP2 Pipeline Driver.

This script provides basic functionality tests and validation checks
for the pipeline driver without requiring actual data files or infrastructure.
"""

import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from qp2.pipelines.pipeline_driver import (
    PipelineDriver, PipelineConfig, DatasetSpec, PipelineResult,
    PipelineType, JobStatus, PipelineFactory, parse_dataset_argument,
    AutoPROCPipeline, Xia2Pipeline, GMCAProcPipeline, StrategyPipeline
)


class TestDatasetSpec(unittest.TestCase):
    """Test DatasetSpec functionality."""
    
    def setUp(self):
        # Create temporary test file
        self.temp_file = tempfile.NamedTemporaryFile(suffix=".h5", delete=False)
        self.temp_file.close()
        
    def tearDown(self):
        # Clean up temporary file
        if os.path.exists(self.temp_file.name):
            os.unlink(self.temp_file.name)
    
    def test_dataset_spec_creation(self):
        """Test DatasetSpec creation and validation."""
        # Valid dataset
        ds = DatasetSpec(self.temp_file.name)
        self.assertEqual(ds.master_file, str(Path(self.temp_file.name).resolve()))
        self.assertIsNone(ds.frame_range)
        
        # With frame range
        ds_range = DatasetSpec(self.temp_file.name, [1, 100])
        self.assertEqual(ds_range.frame_range, [1, 100])
        
        # Invalid file should raise error
        with self.assertRaises(FileNotFoundError):
            DatasetSpec("/nonexistent/file.h5")


class TestPipelineConfig(unittest.TestCase):
    """Test PipelineConfig functionality."""
    
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
    
    def test_config_creation(self):
        """Test PipelineConfig creation with defaults."""
        config = PipelineConfig(work_dir=self.temp_dir)
        
        self.assertEqual(config.work_dir, str(Path(self.temp_dir).resolve()))
        self.assertEqual(config.runner, "slurm")
        self.assertEqual(config.nproc, 8)
        self.assertEqual(config.njobs, 1)
        self.assertIsNotNone(config.username)
        self.assertIsNone(config.highres)
        self.assertEqual(config.pipeline_options, {})
    
    def test_config_with_options(self):
        """Test PipelineConfig with custom options."""
        pipeline_options = {"program": "mosflm", "molsize": 300}
        
        config = PipelineConfig(
            work_dir=self.temp_dir,
            runner="shell",
            nproc=16,
            highres=2.0,
            space_group="P212121",
            fast_mode=True,
            pipeline_options=pipeline_options
        )
        
        self.assertEqual(config.runner, "shell")
        self.assertEqual(config.nproc, 16)
        self.assertEqual(config.highres, 2.0)
        self.assertEqual(config.space_group, "P212121")
        self.assertTrue(config.fast_mode)
        self.assertEqual(config.pipeline_options["program"], "mosflm")


class TestPipelineResult(unittest.TestCase):
    """Test PipelineResult functionality."""
    
    def test_result_creation(self):
        """Test PipelineResult creation and properties."""
        import time
        start_time = time.time()
        
        result = PipelineResult(
            pipeline_type="autoproc",
            job_status=JobStatus.SUCCESS,
            work_dir="/tmp/test",
            start_time=start_time,
            end_time=start_time + 60
        )
        
        self.assertEqual(result.pipeline_type, "autoproc")
        self.assertEqual(result.job_status, JobStatus.SUCCESS)
        self.assertTrue(result.success)
        self.assertAlmostEqual(result.elapsed_time, 60.0, places=1)
        
    def test_result_to_dict(self):
        """Test result dictionary conversion."""
        import time
        result = PipelineResult(
            pipeline_type="xia2",
            job_status=JobStatus.FAILED,
            work_dir="/tmp/test",
            start_time=time.time(),
            error_message="Test error"
        )
        
        result_dict = result.to_dict()
        self.assertEqual(result_dict["pipeline_type"], "xia2")
        self.assertEqual(result_dict["job_status"], "FAILED")
        self.assertFalse(result_dict["success"])
        self.assertEqual(result_dict["error_message"], "Test error")


class TestPipelineFactory(unittest.TestCase):
    """Test PipelineFactory functionality."""
    
    def setUp(self):
        self.temp_file = tempfile.NamedTemporaryFile(suffix=".h5", delete=False)
        self.temp_file.close()
        self.temp_dir = tempfile.mkdtemp()
        
        self.datasets = [DatasetSpec(self.temp_file.name)]
        self.config = PipelineConfig(work_dir=self.temp_dir)
    
    def tearDown(self):
        if os.path.exists(self.temp_file.name):
            os.unlink(self.temp_file.name)
    
    def test_supported_pipelines(self):
        """Test getting supported pipeline types."""
        supported = PipelineFactory.get_supported_pipelines()
        expected = ["autoproc", "xia2", "gmcaproc", "strategy"]
        
        self.assertEqual(set(supported), set(expected))
    
    def test_create_pipeline_from_enum(self):
        """Test creating pipeline from PipelineType enum."""
        pipeline = PipelineFactory.create_pipeline(
            PipelineType.AUTOPROC, self.datasets, self.config
        )
        self.assertIsInstance(pipeline, AutoPROCPipeline)
    
    def test_create_pipeline_from_string(self):
        """Test creating pipeline from string."""
        pipeline = PipelineFactory.create_pipeline(
            "xia2", self.datasets, self.config
        )
        self.assertIsInstance(pipeline, Xia2Pipeline)
    
    def test_invalid_pipeline_type(self):
        """Test error handling for invalid pipeline type."""
        with self.assertRaises(ValueError):
            PipelineFactory.create_pipeline(
                "invalid_pipeline", self.datasets, self.config
            )


class TestDatasetArgumentParsing(unittest.TestCase):
    """Test dataset argument parsing functionality."""
    
    def setUp(self):
        self.temp_file = tempfile.NamedTemporaryFile(suffix=".h5", delete=False)
        self.temp_file.close()
    
    def tearDown(self):
        if os.path.exists(self.temp_file.name):
            os.unlink(self.temp_file.name)
    
    def test_parse_simple_dataset(self):
        """Test parsing simple dataset argument."""
        ds = parse_dataset_argument(self.temp_file.name)
        self.assertEqual(ds.master_file, str(Path(self.temp_file.name).resolve()))
        self.assertIsNone(ds.frame_range)
    
    def test_parse_dataset_with_range(self):
        """Test parsing dataset argument with frame range."""
        arg = f"{self.temp_file.name}:10:100"
        ds = parse_dataset_argument(arg)
        self.assertEqual(ds.frame_range, [10, 100])
    
    def test_parse_invalid_range(self):
        """Test error handling for invalid frame range."""
        arg = f"{self.temp_file.name}:invalid:range"
        with self.assertRaises(ValueError):
            parse_dataset_argument(arg)
        
        arg = f"{self.temp_file.name}:1:2:3:4"
        with self.assertRaises(ValueError):
            parse_dataset_argument(arg)


class TestPipelineDriver(unittest.TestCase):
    """Test PipelineDriver functionality."""
    
    def setUp(self):
        self.driver = PipelineDriver()
        self.temp_file = tempfile.NamedTemporaryFile(suffix=".h5", delete=False)
        self.temp_file.close()
        self.temp_dir = tempfile.mkdtemp()
    
    def tearDown(self):
        if os.path.exists(self.temp_file.name):
            os.unlink(self.temp_file.name)
    
    def test_driver_initialization(self):
        """Test driver initialization."""
        self.assertEqual(len(self.driver.results_history), 0)
    
    def test_get_results_summary_empty(self):
        """Test results summary with no runs."""
        summary = self.driver.get_results_summary()
        
        self.assertEqual(summary["total_runs"], 0)
        self.assertEqual(summary["successful_runs"], 0) 
        self.assertEqual(summary["failed_runs"], 0)
        self.assertEqual(summary["success_rate"], 0.0)
        self.assertEqual(len(summary["recent_runs"]), 0)


class MockPipelineTests(unittest.TestCase):
    """Test pipeline implementations with mocked dependencies."""
    
    def setUp(self):
        self.temp_file = tempfile.NamedTemporaryFile(suffix=".h5", delete=False)
        self.temp_file.close()
        self.temp_dir = tempfile.mkdtemp()
        
        self.datasets = [DatasetSpec(self.temp_file.name)]
        self.config = PipelineConfig(work_dir=self.temp_dir)
    
    def tearDown(self):
        if os.path.exists(self.temp_file.name):
            os.unlink(self.temp_file.name)
    
    def test_autoproc_validation(self):
        """Test AutoPROC input validation."""
        pipeline = AutoPROCPipeline(self.datasets, self.config)
        self.assertTrue(pipeline._validate_inputs())
        
        # Test with non-HDF5 file
        bad_datasets = [DatasetSpec("/tmp/test.cbf")]
        with patch('os.path.exists', return_value=True):
            pipeline = AutoPROCPipeline(bad_datasets, self.config)
            self.assertFalse(pipeline._validate_inputs())
    
    def test_xia2_validation(self):
        """Test Xia2 input validation.""" 
        pipeline = Xia2Pipeline(self.datasets, self.config)
        self.assertTrue(pipeline._validate_inputs())
    
    def test_strategy_validation(self):
        """Test Strategy input validation."""
        config = PipelineConfig(
            work_dir=self.temp_dir,
            pipeline_options={"program": "mosflm"}
        )
        pipeline = StrategyPipeline(self.datasets, config)
        self.assertTrue(pipeline._validate_inputs())
        
        # Test with invalid program
        bad_config = PipelineConfig(
            work_dir=self.temp_dir,
            pipeline_options={"program": "invalid"}
        )
        bad_pipeline = StrategyPipeline(self.datasets, bad_config)
        self.assertFalse(bad_pipeline._validate_inputs())
    
    def test_command_construction(self):
        """Test command construction for different pipelines."""
        # AutoPROC
        autoproc = AutoPROCPipeline(self.datasets, self.config)
        cmd = autoproc._construct_command()
        self.assertIn("process", cmd)
        self.assertIn("-h5", cmd)
        
        # Xia2
        xia2 = Xia2Pipeline(self.datasets, self.config)
        cmd = xia2._construct_command()
        self.assertIn("xia2", cmd)
        self.assertIn("image=", cmd)
        
        # Strategy
        config = PipelineConfig(
            work_dir=self.temp_dir,
            pipeline_options={"program": "mosflm"}
        )
        strategy = StrategyPipeline(self.datasets, config)
        cmd = strategy._construct_command()
        self.assertIn("--program mosflm", cmd)


def run_basic_tests():
    """Run basic functionality tests."""
    print("Running QP2 Pipeline Driver Tests...")
    print("=" * 50)
    
    # Create test suite
    suite = unittest.TestSuite()
    
    # Add test cases
    suite.addTest(unittest.makeSuite(TestDatasetSpec))
    suite.addTest(unittest.makeSuite(TestPipelineConfig))
    suite.addTest(unittest.makeSuite(TestPipelineResult))
    suite.addTest(unittest.makeSuite(TestPipelineFactory))
    suite.addTest(unittest.makeSuite(TestDatasetArgumentParsing))
    suite.addTest(unittest.makeSuite(TestPipelineDriver))
    suite.addTest(unittest.makeSuite(MockPipelineTests))
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    # Print summary
    print(f"\n{'='*50}")
    print(f"Tests run: {result.testsRun}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    
    if result.failures:
        print("\nFailures:")
        for test, traceback in result.failures:
            print(f"  {test}: {traceback}")
    
    if result.errors:
        print("\nErrors:")
        for test, traceback in result.errors:
            print(f"  {test}: {traceback}")
    
    success = len(result.failures) == 0 and len(result.errors) == 0
    print(f"\nOverall result: {'PASSED' if success else 'FAILED'}")
    
    return success


def run_integration_test():
    """Run a basic integration test with mocked components."""
    print("\n" + "=" * 50)
    print("Running Integration Test...")
    
    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create mock data file
            temp_file = os.path.join(temp_dir, "test_master.h5")
            with open(temp_file, 'w') as f:
                f.write("mock hdf5 data")
            
            # Mock the pipeline execution components
            with patch('qp2.pipelines.pipeline_driver.PipelineTracker') as mock_tracker, \
                 patch('qp2.pipelines.pipeline_driver.run_command') as mock_run_cmd:
                
                # Set up mocks
                mock_tracker_instance = Mock()
                mock_tracker_instance.pipeline_status_id = 12345
                mock_tracker_instance.result_pk_value = 67890
                mock_tracker.return_value = mock_tracker_instance
                
                # Create mock result files
                work_dir = os.path.join(temp_dir, "test_work")
                os.makedirs(work_dir, exist_ok=True)
                
                # Create configuration
                config = PipelineConfig(
                    work_dir=work_dir,
                    runner="shell",
                    sample_name="test_sample",
                    nproc=2
                )
                
                datasets = [DatasetSpec(temp_file)]
                
                # Test pipeline factory
                pipeline = PipelineFactory.create_pipeline("autoproc", datasets, config)
                print(f"✓ Created {type(pipeline).__name__}")
                
                # Mock successful parsing
                with patch.object(pipeline, '_parse_results', return_value={"test": "success"}):
                    # Test basic driver functionality
                    driver = PipelineDriver()
                    result = driver.run_pipeline("autoproc", datasets, config)
                    
                    print(f"✓ Pipeline executed with status: {result.job_status.value}")
                    print(f"✓ Work directory: {result.work_dir}")
                    print(f"✓ Results history length: {len(driver.results_history)}")
                    
                    # Test results summary
                    summary = driver.get_results_summary()
                    print(f"✓ Summary - Total runs: {summary['total_runs']}")
        
        print("✓ Integration test completed successfully")
        return True
        
    except Exception as e:
        print(f"✗ Integration test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Run all tests."""
    print("QP2 Pipeline Driver Test Suite")
    print("=" * 60)
    
    # Run unit tests
    unit_test_success = run_basic_tests()
    
    # Run integration test
    integration_test_success = run_integration_test()
    
    # Overall result
    print(f"\n{'='*60}")
    overall_success = unit_test_success and integration_test_success
    print(f"OVERALL TEST RESULT: {'PASSED' if overall_success else 'FAILED'}")
    
    if overall_success:
        print("\n✓ All tests passed! Pipeline driver is ready for use.")
        print("  Next steps:")
        print("  1. Test with real data files")
        print("  2. Verify SLURM integration")  
        print("  3. Check database connectivity")
        print("  4. Run example scripts")
    else:
        print("\n✗ Some tests failed. Please review and fix issues.")
    
    return 0 if overall_success else 1


if __name__ == "__main__":
    sys.exit(main())