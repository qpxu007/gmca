curl -X POST -H "Content-Type: application/json" -d '{
    "pipeline": "my_custom_pipeline",
    "proc_dir": "/path/to/processing_directory",
    "data_dir": "/path/to/data_directory",
    "sample_id": "sample123",
    "username": "testuser",
    "groupname": "testgroup"
    # ... any other parameters required by xprocess ...
}' http://localhost:8080/launch_job
