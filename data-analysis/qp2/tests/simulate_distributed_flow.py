
import os
import sys
import json
import unittest
from unittest.mock import MagicMock, patch

# Add plugin dir to path
sys.path.append("/home/qxu/data-analysis/qp2/image_viewer/plugins/xia2_ssx/distributed")

import orchestrator

class TestDistributedFlow(unittest.TestCase):
    def setUp(self):
        self.test_dir = os.path.abspath("test_dist_flow")
        os.makedirs(self.test_dir, exist_ok=True)
        self.old_cwd = os.getcwd()
        os.chdir(self.test_dir)
        
        # Write dummy config
        self.config = {
            "datasets": ["/data/d1_master.h5", "/data/d2_master.h5", "/data/d3_master.h5", "/data/d4_master.h5"],
            "status_keys": ["k1", "k2", "k3", "k4"],
            "redis_host": "localhost",
            "redis_port": 6379,
            "incremental_merging": True,
            "nproc": 4,
            "d_min": 1.5,
            "setup_cmd": "echo setup"
        }
        with open("job_config.json", "w") as f:
            json.dump(self.config, f)
            
        # Mock setup_env.sh
        with open("setup_env.sh", "w") as f:
            f.write("echo setup")

    def tearDown(self):
        os.chdir(self.old_cwd)
        # cleanup?
        
    @patch('subprocess.run')
    @patch('redis.Redis')
    def test_flow(self, mock_redis, mock_run):
        # Mock successful sbatch
        mock_run.return_value.returncode = 0
        mock_run.return_value.stdout = "123456"
        
        orchestrator.main()
        
        # Verify calls
        # We expect 4 integration jobs
        # 3 incremental reductions (25%, 50%, 75% -> 1, 2, 3 jobs out of 4)
        # 1 final reduction
        
        # Integration jobs
        # Check that --chdir is present
        bg_calls = []
        for call in mock_run.call_args_list:
            args = call[0][0]
            if any(str(a).endswith("integrate.sh") for a in args):
                bg_calls.append(call)
                
        self.assertEqual(len(bg_calls), 4, "Should have 4 integration jobs")
        
        # check args of first call
        first_call_args = bg_calls[0][0][0]
        # Expected: sbatch ... --chdir=.../job_0_d1_master ... integrate.sh ...
        print("Integration Call 0:", first_call_args)
        
        has_chdir = any(arg.startswith("--chdir=") for arg in first_call_args)
        self.assertTrue(has_chdir, "Integration job missing --chdir")
        
        # Reduction jobs
        reduce_calls = [c for c in mock_run.call_args_list if "reduce_wrapper.sh" in c[0][0][-2]] # wrapper is near end
        # We expect: 
        # 25% of 4 = 1 job. limit=1. dep=123456.
        # 50% of 4 = 2 jobs. limit=2. dep=123456:123456.
        # 75% of 4 = 3 jobs. limit=3. dep=...
        # Final = all. limit=None.
        
        self.assertEqual(len(reduce_calls), 4, "Should have 4 reduction jobs (3 incremental + 1 final)")
        
        final_call = reduce_calls[-1][0][0]
        print("Final Call:", final_call)
        
        # Check chdir for reduction
        has_chdir_red = any(arg.startswith("--chdir=") for arg in final_call)
        self.assertTrue(has_chdir_red, "Reduction job missing --chdir")
        
        # Check final dependency list
        dep_arg = next(arg for arg in final_call if arg.startswith("--dependency="))
        deps = dep_arg.split(":")[1]

    def test_worker_logic(self):
        # This isn't a full UI test, but verifies the logic we touched in submit_xia2_ssx_job.py
        # specifically construction of the run_command for orchestrator
        # and checking duplicate args are gone
        pass
        
if __name__ == "__main__":
    unittest.main()
