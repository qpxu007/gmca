# How User Datasets are Discovered and Filtered

This document explains the mechanism by which the GMCA Web Apps identify and display datasets belonging to a specific user.

## 1. User Identification
When a user logs in, they receive a **JSON Web Token (JWT)**. This token contains their `username`. For every subsequent request to the API (like fetching the dataset list), this token is sent in the `Authorization` header.

The backend extracts and verifies this token to identify the **current user**.

## 2. The Database Model
The core of this system is the `DatasetRun` model (mapped to the `dataset_runs` table in the database). This table contains a record for every data collection run.

Crucially, it has a **`username` column**. This column stores the ID of the user who owns or collected the data.

**Example Database Record:**
| data_id | username | run_prefix | ... |
| :--- | :--- | :--- | :--- |
| 101 | **jdoe** | run_A_001 | ... |
| 102 | **bsmith** | run_B_005 | ... |

## 3. Filtering Logic (The API Endpoint)
The logic resides in `web_app/backend/dataset_routes.py`, specifically within the `list_datasets` function.

### Step-by-Step Flow:

1.  **Receive Request:** The endpoint receives a request (e.g., `GET /datasets/list`).
2.  **Check Permissions:** It checks if the current user is a "Staff Member" (admin).
    *   `is_staff = is_staff_member(user)`
3.  **Apply Filter:**
    *   **If Staff:** No filter is applied. The query selects **all** records from `dataset_runs`.
    *   **If Regular User:** The query applies a strict filter:
        ```python
        query = query.filter(DatasetRun.username == user)
        ```
        This SQL equivalent is `SELECT * FROM dataset_runs WHERE username = 'jdoe'`.

### Result
*   **User 'jdoe'** will only receive JSON data for run `101`.
*   **User 'bsmith'** will only receive run `102`.
*   **Staff/Admin** will receive both `101` and `102`.

## 4. Source of Data
The `dataset_runs` table is populated by the beamline data acquisition system. When a data collection run finishes, the system inserts a row into this table, explicitly tagging it with the current user's username.

In our development environment, the `seed_datasets.py` script mimics this by creating random entries assigned to users like `user1`, `guest`, and `admin`.
