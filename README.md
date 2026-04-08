## Snowflake - Deferred Merge - Brief Summary

- **Deferred Merge - Append Stream** Streamlines high-frequency data ingestion by capturing incremental changes (CDC) from source tables or views. These nodes efficiently stage new records, updates, and deletes into a buffer, providing the necessary agility for real-time pipelines while shielding large base tables from the performance overhead of constant micro-partition rewrites.
- **Deferred Merge - Delta Stream** Manages the intermediate delta layer by processing change streams into a queryable, state-aware buffer. By handling record versioning and complex DML logic natively, these nodes ensure that the most recent record states are immediately available via hybrid views, maintaining a "single source of truth" while deferring heavy-duty merge operations to optimized, lower-frequency schedules.

----

## Nodetypes Config Matrix

| Category | Feature | Append Stream | Delta Stream |
| :--- | :--- | :---: | :---: |
| **General** | Development Mode  | ✅ | ✅ |
| **General** | Create Target As (Table/Transient) | ✅ | ✅ |
| **Stream** | Source Object  | ✅ | ✅ |
| **Stream** | Show Initial Rows | ✅ | ✅ |
| **Stream** | Redeployment Behavior | ✅ | ✅ |
| **Loading** | Table Keys (Business Keys) | ✅ | ✅ |
| **Loading** | Record Versioning (Latest Tracking) | ✅ | ✅ |
| **DML Ops** | Column Identifier | ✅ | ✅ |
| **DML Ops** | Include Value for Update | ✅ | ✅ |
| **DML Ops** | Insert Value | ✅ | ✅ |
| **DML Ops** | Delete Value | ✅ | ✅ |
| **Delete** | Soft Delete Toggle | ✅ | ✅ |
| **Delete** | Retain Last Non-Deleted Values | ✅ | ✅ |
| **Clustering** | Cluster Key Support | ✅ | ✅ |
| **Clustering** | Allow Expressions in Cluster Key | ✅ | ✅ |
| **Scheduling** | Scheduling Mode | ✅ | ✅ |
| **Scheduling** | When Source Stream has Data Flag | ✅ | ✅ |
| **Scheduling** | Warehouse Selection | ✅ | ✅ |
| **Scheduling** | Initial Serverless Size | ✅ | ✅ |
| **Scheduling** | Task Schedule (Minutes/Cron/Predecessor) | ✅ | ✅ |
| **Scheduling** | Predecessor Task Management | ✅ | ✅ |
| **Scheduling** | Root Task Name Selection | ✅ | ✅ |


---

The Deferred Merge Package includes mechanisms for handling merge operations with different streaming strategies:

* [Deferred Merge Append Stream](#deferred-merge-append-stream)
* [Deferred Merge Delta Stream](#deferred-merge-delta-stream)

---

## Deferred Merge Append Stream

The Deferred Merge - Append Stream Node includes several key steps to ensure efficient and up-to-date data processing. First, a stream is created to capture row inserts. Then a target table is created and initially loaded with data. A hybrid view is established to provide access to the most current data by combining initial updates.

Finally, a scheduled task manages ongoing updates by merging changes into the target table. This process ensures that the target table remains synchronized with the source data maintaining accuracy and timeliness.

### Deferred Merge Append Stream Node Configuration

The Deferred Merge Append node has the following configuration groups:

* [General Options](#append-stream-general-options)  
* [Stream Options](#append-stream-options)
* [Target Loading Options](#append-stream-target-loading-options)
* [Target Row DML Operations](#append-stream-target-row-dml-operations)
* [Target Delete Options](#append-stream-target-delete-options)
* [Target Clustering Options](#append-stream-target-clustering-options)
* [Scheduling Options](#append-stream-scheduling-options)

#### Append Stream General Options

![Append General Options](https://github.com/coalesceio/Deferred-Merge/assets/169126315/e6a7d6a5-da3b-4001-b84f-24e2a85708e7)

| **Option** | **Description** |
|------------|----------------|
| **Development Mode** | True/False toggle that determines task creation:<br/>- **True**: Table created and SQL executes as Run action<br/>- **False**: SQL wraps into task with Scheduling Options. Displays a message prompting the user to wait or run manually when executed. |
| **Create Target As** | Choose target object type:<br/>- **Table**: Creates table<br/>- **Transient Table**: Creates transient table |

Prior to creating a task, it is helpful to test the SQL the task will execute to make sure it runs without errors and returns the expected data.

#### Append Stream Options

![Append Stream Options](https://github.com/coalesceio/Deferred-Merge/assets/169126315/c4daf590-5a13-49bd-b389-3b3cdc8f40af)

| **Option** | **Description** |
|------------|----------------|
| **Source Object** | Type of object for stream creation (Required):<br/>- **Table**<br/>- **View** |
| **Show Initial Rows** | **True**: Returns only rows that existed when stream created<br/>**False**: Returns DML changes since most recent offset |
| **Redeployment Behavior** | Determines stream recreation behavior |

| **Redeployment Behavior** | **Stage Executed** |
|---|---|
| **Create Stream if not exists**| Re-Create Stream at existing offset|
| **Create or Replace** | Create Stream|
| **Create at existing stream**|  Re-Create Stream at existing offset |

#### Append Stream Target Loading Options

![Append Target Loading Options](https://github.com/coalesceio/Deferred-Merge/assets/169126315/77856b16-3bd6-47f5-a8ad-ffc2f73888dd)

| **Option** | **Description** |
|------------|----------------|
| **Table keys** | Business keys columns for merging into target table |
| **Record Versioning** | Date Time or Date and Timestamp column for latest record tracking |

#### Append Stream Target Row DML Operations

![Append Target Row DML Operations](https://github.com/coalesceio/Deferred-Merge/assets/169126315/507731aa-d5a1-49b9-9b14-bc7dbf335cab)

| **Option** | **Description** |
|------------|----------------|
| **Column Identifier** | Column identifying DML operations |
| **Include Value for Update** |  For records flagged under Update, the existing records in the target table are updated with the corresponding values from the source table. |
| **Insert Value** | It indicates that the corresponding record is meant to be inserted into the target table. This condition ensures that only records flagged for insertion are actually inserted into the target table during the merge operation.|
| **Delete Value** | This value indicates that the corresponding record should either be soft-deleted (if the condition is met by enabling the soft delete toggle) or hard-deleted from the target table. |

#### Append Stream Target Delete Options

![Append Target Delete Options](https://github.com/coalesceio/Deferred-Merge/assets/169126315/6fe8d21b-80ea-41b0-a56a-5f9ccb7b3874)

| **Option** | **Description** |
|------------|----------------|
| **Soft Delete** | Toggle to maintain deleted data record |
| **Retain Last Non-Deleted Values** | Preserves most recent non-deleted record, even as other records are marked as deleted or become inactive. |

#### Append Stream Target Clustering Options

![Append Target Clustering Options](https://github.com/coalesceio/Deferred-Merge/assets/169126315/6b168402-3bec-47d9-9870-be72e871f404)

| **Option** | **Description** |
|------------|----------------|
| **Cluster key** | **True**: Specify clustering column and allow expressions<br/>**False**: No clustering implemented |
| **Allow Expressions Cluster Key**| Aadd an expression to the specified cluster key|

#### Append Stream Scheduling Options

If development mode is set to false then Scheduling Options can be used to configure how and when the task will run.

<img width="436" height="596" alt="image" src="https://github.com/user-attachments/assets/30e1b229-6df1-447b-9b22-8bc42ea78d8c" />

| **Option** | **Description** |
|------------|----------------|
| **Scheduling Mode** | Choose compute type:<br/>- **Warehouse Task**: User managed warehouse executes tasks<br/>- **Serverless Task**: Uses serverless compute |
| **When Source Stream has Data Flag** | True/False toggle to check for stream data<br/>**True** - Only run task if source stream has capture change data<br/>**False** -  Run task on schedule regardless of whether the source stream has data. If the source is not a stream should set this to false. |
| **Select Warehouse** | Visible if Scheduling Mode is set to Warehouse Task. Enter the name of the warehouse you want the task to run on without quotes.|
| **Select initial serverless size** | Visible when Scheduling Mode is set to Serverless Task.<br/> Select the initial compute size on which to run the task. Snowflake will adjust size from there based on target schedule and task run times. |
| **Enable Size Bounds** | Toggle to set explicit limits on serverless scaling. (Visible if **Serverless Task** is selected).<br/>**Validation Rules:**<br/>- Min size must be ≤ Initial size<br/>- Max size must be ≥ Initial size<br/>- Min size must be ≤ Max size |
| **Minimum Warehouse Size** | The smallest compute size allowed for the task (e.g., 1. XSMALL). |
| **Maximum Warehouse Size** | The largest compute size allowed for the task (e.g., 6. XXLARGE). |
| **Task Schedule** | Choose schedule type:<br/>- **Minutes** - Specify interval in minutes. Enter a whole number from 1 to 11520 which represents the number of minutes between task runs.<br/>- **Cron** - Uses [Cron expressions](https://docs.coalesce.io/docs/reference/cron-reference/). Specifies a cron expression and time zone for periodically running the task. Supports a subset of standard cron utility syntax.<br/>- **Predecessor** - Specify dependent tasks |
| **Execution Time** | The specific duration for the task run limit. Supported ranges:<br/>- **SECONDS**: 10 - 691200<br/>- **MINUTES**: 1 - 11520<br/>- **HOURS**: 1 - 192 |
| **Enter predecessor tasks separated by a comma**| Visible when Task Schedule is set to Predecessor. <br/>One or more task names that precede the task being created in the current node. Task names are case sensitive, should not be quoted and must exist in the same schema in which the current task is being created. If there are multiple predecessor task separate the task names using a comma and no spaces.|
| **Root task name** | Visible when Task Schedule is set to Predecessor.<br/> Name of the root task that controls scheduling for the DAG of tasks. Task names are case sensitive, should not be quoted and must exist in the same schema in which the current task is being created. If there are multiple predecessor task separate the task names using a comma and no spaces.|

#### Append Stream Advanced Scheduling Options

<img width="465" height="708" alt="image" src="https://github.com/user-attachments/assets/44c937fa-3827-4ab7-9480-8e23a8fb89e4" />

| **Option** | **Description** |
|------------|----------------|
| **Execute As Specific User** | Toggle to run on behalf of another user. Requires `GRANT IMPERSONATE` privileges. |
| **User Name** | The specific user account name used when **Execute As Specific User** is enabled. |
| **Allow Overlapping Execution** | Allows a new instance of the task to start if the previous one is still running. |
| **Enable Task Graph Config** | Enables a text box to provide **Configuration JSON** for the task graph. |
| **Auto-Suspend After Failures** | Automatically suspends the task after a set number of consecutive failures. |
| **Number of Consecutive Failures** | Set the threshold (0 - No Limit) before the task is automatically suspended. <br/>- When toggle is OFF: Parameter is not included (uses Snowflake default of 10).<br/>- When toggle is ON with value 0: **Disables** auto-suspension.<br/>- When toggle is ON with value > 0: **Suspends** after that many consecutive failures. |
| **Enable Auto-Retry** | Toggle to automatically retry the task if it fails. |
| **Retry Attempts** | Specify the number of retry attempts allowed (Range: 0 - 30). |

#### Append Stream Notification Options

<img width="453" height="459" alt="image" src="https://github.com/user-attachments/assets/83b9dcfc-20c9-41c0-a459-a8706b3c885f" />

| **Option** | **Description** |
|------------|----------------|
| **Enable Error Notifications** | Toggle to send alerts on failure. Requires an **Error Integration Name**. |
| **Enable Success Notifications** | Toggle to send alerts on success. Requires a **Success Integration Name**. |

> **Note:** Options under **Advanced Scheduling Options** and **Notification Options** (Execution Time, Overlapping Execution, Auto-Suspend, Auto-Retry, etc.) are only applicable to **Root** and **Independent** tasks. The only exception is **Execute As Specific User**, which can be configured for any task in the graph.


### Append Stream Limitations

> 🚧 **Appyling Transformation**
> This node can't apply transformations to the columns for this node type.

### Append Stream Deployment

#### Append Stream Parameters

It includes an environment parameter that allows you to specify a different warehouse to run a task in different environments.
The parameter name is `targetTaskWarehouse`, and the default value is `DEV ENVIRONMENT`.

When set to `DEV ENVIRONMENT`, the value entered in the Scheduling Options config "Select Warehouse on which to run the task" will be used when creating the task.

```json
{"targetTaskWarehouse": "DEV ENVIRONMENT"}
```

When set to any value other than `DEV ENVIRONMENT`, the node will attempt to create the task using a Snowflake warehouse with the specified value.
For example, with the setting below for the parameter in a QA environment, the task will execute using a warehouse named `compute_wh`.

```json
{"targetTaskWarehouse": "compute_wh"}
```

#### Append Stream Initial Deployment

When deployed for the first time into an environment, executes the following stages:

| **Stage** | **Description** |
|-----------|----------------|
| **Create Stream** | Executes CREATE OR REPLACE statement to create Stream in target environment |
| **Create Target Table** | Creates destination table for processed data storage |
| **Target Table Initial Load** | Populates the target table with the existing data from the source object. This step ensures that the target table starts with a complete set of data before any further changes are applied |
| **Create Hybrid View** | Provides access to the most up-to-date data by combining the initial data load with any subsequent changes captured by the stream. The hybrid view ensures that users always have access to the latest version of the data without waiting for batch updates.|
| **Create Task** | Creates merge operation task for stream changes |
| **Resume Task** | Activates the created task for scheduled execution |
| **Apply Table Clustering** | Alters table with cluster key if enabled |
| **Resume Recluster Clustering** | Enables periodic reclustering to maintain optimal clustering |

##### Append Stream Predecessor Task Deployment

When deploying with predecessor tasks, executes:

| **Stage** | **Description** |
|-----------|----------------|
| **Suspend Root Task** | Suspends root task to add task into DAG |
| **Create Task** | Creates task for loading target table |

> 📘 **Task DAG Note**
>
> For tasks in a DAG, include Task Dag Resume Root node type to resume root node after dependent task creation. Tasks use CREATE OR REPLACE only, with no ALTER capabilities.

##### Append Stream Redeployment

| **Redeployment Behavior** | **Stage Executed** |
|--------------------------|-------------------|
| **Create Stream if not exists** | Re-Create Stream at existing offset |
| **Create or Replace** | Create Stream |
| **Create at existing stream** | Re-Create Stream at existing offset |

##### Append Stream Table Redeployment

The following column/table changes trigger ALTER statements:

* Table name changes
* Column drops
* Column data type alterations
* New column additions

Executes these stages:

| **Stage** | **Description** |
|-----------|----------------|
| **Alter Table Operations** | Executes appropriate ALTER statements for schema changes |
| **Target Initial Load** | Executes load based on configuration:<br/>- If initial load enabled + "Create or Replace": Uses INSERT OVERWRITE<br/>- All other scenarios: Uses INSERT INTO |

##### Append Stream View Redeployment

Stream or table changes trigger Hybrid View recreation.

##### Append Stream Task Redeployment

Stream or table changes trigger:

1. Task recreation
2. Task resumption


> 🚧 Redeployment Behavior
>
> Redeployment with changes in Stream/Table/Task properties will result in execution of all steps mentioned in inital deployment.

#### Node Type Switching

Node Type switching is supported starting from Coalesce version **7.28+**.

From this version onward, a node’s materialization type can be switched from one supported type to another, subject to certain limitations.

For more info click here - [Node Type Switching Logic and Limitations](#node-type-switching-logic)

### Append Stream Undeployment

When node is deleted, executes:

* Drop Stream
* Drop Table/Transient Table  
* Drop View
* Drop Current Task

---

## Deferred Merge Delta Stream

The Deferred Merge - Delta Stream Node includes several key steps to ensure efficient and up-to-date data processing. First, a stream is created to capture changes from the source object to tracks all DML changes to the source object, including inserts, updates, and deletes. Then a target table is created and initially loaded with data. A hybrid view is established to provide access to the most current data by combining initial updates.

Finally, a scheduled task manages ongoing updates by merging changes into the target table. This process ensures that the target table remains synchronized with the source data maintaining accuracy and timeliness

### Deferred Merge Delta Stream Node Configuration

The Deferred Merge Append node has the following configuration groups:

* [General Options](#append-stream-general-options)  
* [Stream Options](#append-stream-options)
* [Target Loading Options](#append-stream-target-loading-options)
* [Target Row DML Operations](#append-stream-target-row-dml-operations)
* [Target Delete Options](#append-stream-target-delete-options)
* [Target Clustering Options](#append-stream-target-clustering-options)
* [Scheduling Options](#append-stream-scheduling-options)

#### Delta Stream Stream General Options

![Append General Options](https://github.com/coalesceio/Deferred-Merge/assets/169126315/e6a7d6a5-da3b-4001-b84f-24e2a85708e7)

| **Option** | **Description** |
|------------|----------------|
| **Development Mode** | True/False toggle that determines task creation:<br/>- **True**: Table created and SQL executes as Run action<br/>- **False**: SQL wraps into task with Scheduling Options. Displays a message prompting the user to wait or run manually when executed. |
| **Create Target As** | Choose target object type:<br/>- **Table**: Creates table<br/>- **Transient Table**: Creates transient table |

Prior to creating a task, it is helpful to test the SQL the task will execute to make sure it runs without errors and returns the expected data.

#### Delta Stream Options

![Append Stream Options](https://github.com/coalesceio/Deferred-Merge/assets/169126315/c4daf590-5a13-49bd-b389-3b3cdc8f40af)

| **Option** | **Description** |
|------------|----------------|
| **Source Object** | Type of object for stream creation (Required):<br/>- **Table**<br/>- **View** |
| **Show Initial Rows** | **True**: Returns only rows that existed when stream created<br/>**False**: Returns DML changes since most recent offset |
| **Redeployment Behavior** | Determines stream recreation behavior |

| **Redeployment Behavior** | **Stage Executed** |
|---|---|
| **Create Stream if not exists**| Re-Create Stream at existing offset|
| **Create or Replace** | Create Stream|
| **Create at existing stream**|  Re-Create Stream at existing offset |

#### Delta Stream Target Loading Options

![Append Target Loading Options](https://github.com/coalesceio/Deferred-Merge/assets/169126315/77856b16-3bd6-47f5-a8ad-ffc2f73888dd)

| **Option** | **Description** |
|------------|----------------|
| **Table keys** | Business keys columns for merging into target table |
| **Record Versioning** | Date Time or Date and Timestamp column for latest record tracking |

#### Delta Stream Target Row DML Operations

![Append Target Row DML Operations](https://github.com/coalesceio/Deferred-Merge/assets/169126315/507731aa-d5a1-49b9-9b14-bc7dbf335cab)

| **Option** | **Description** |
|------------|----------------|
| **Column Identifier** | Column identifying DML operations |
| **Include Value for Update** |  For records flagged under Update, the existing records in the target table are updated with the corresponding values from the source table. |
| **Insert Value** | It indicates that the corresponding record is meant to be inserted into the target table. This condition ensures that only records flagged for insertion are actually inserted into the target table during the merge operation.|
| **Delete Value** | This value indicates that the corresponding record should either be soft-deleted (if the condition is met by enabling the soft delete toggle) or hard-deleted from the target table. |

#### Delta Stream Target Delete Options

![Append Target Delete Options](https://github.com/coalesceio/Deferred-Merge/assets/169126315/6fe8d21b-80ea-41b0-a56a-5f9ccb7b3874)

| **Option** | **Description** |
|------------|----------------|
| **Soft Delete** | Toggle to maintain deleted data record |
| **Retain Last Non-Deleted Values** | Preserves most recent non-deleted record, even as other records are marked as deleted or become inactive. |

#### Delta Stream Target Clustering Options

![Append Target Clustering Options](https://github.com/coalesceio/Deferred-Merge/assets/169126315/6b168402-3bec-47d9-9870-be72e871f404)

| **Option** | **Description** |
|------------|----------------|
| **Cluster key** | **True**: Specify clustering column and allow expressions<br/>**False**: No clustering implemented |
| **Allow Expressions Cluster Key**| Aadd an expression to the specified cluster key|

#### Delta Stream Scheduling Options

If development mode is set to false then Scheduling Options can be used to configure how and when the task will run.

<img width="436" height="596" alt="image" src="https://github.com/user-attachments/assets/30e1b229-6df1-447b-9b22-8bc42ea78d8c" />

| **Option** | **Description** |
|------------|----------------|
| **Scheduling Mode** | Choose compute type:<br/>- **Warehouse Task**: User managed warehouse executes tasks<br/>- **Serverless Task**: Uses serverless compute |
| **When Source Stream has Data Flag** | True/False toggle to check for stream data<br/>**True** - Only run task if source stream has capture change data<br/>**False** -  Run task on schedule regardless of whether the source stream has data. If the source is not a stream should set this to false. |
| **Select Warehouse** | Visible if Scheduling Mode is set to Warehouse Task. Enter the name of the warehouse you want the task to run on without quotes.|
| **Select initial serverless size** | Visible when Scheduling Mode is set to Serverless Task.<br/> Select the initial compute size on which to run the task. Snowflake will adjust size from there based on target schedule and task run times. |
| **Enable Size Bounds** | Toggle to set explicit limits on serverless scaling. (Visible if **Serverless Task** is selected).<br/>**Validation Rules:**<br/>- Min size must be ≤ Initial size<br/>- Max size must be ≥ Initial size<br/>- Min size must be ≤ Max size |
| **Minimum Warehouse Size** | The smallest compute size allowed for the task (e.g., 1. XSMALL). |
| **Maximum Warehouse Size** | The largest compute size allowed for the task (e.g., 6. XXLARGE). |
| **Task Schedule** | Choose schedule type:<br/>- **Minutes** - Specify interval in minutes. Enter a whole number from 1 to 11520 which represents the number of minutes between task runs.<br/>- **Cron** - Uses [Cron expressions](https://docs.coalesce.io/docs/reference/cron-reference/). Specifies a cron expression and time zone for periodically running the task. Supports a subset of standard cron utility syntax.<br/>- **Predecessor** - Specify dependent tasks |
| **Execution Time** | The specific duration for the task run limit. Supported ranges:<br/>- **SECONDS**: 10 - 691200<br/>- **MINUTES**: 1 - 11520<br/>- **HOURS**: 1 - 192 |
| **Enter predecessor tasks separated by a comma**| Visible when Task Schedule is set to Predecessor. <br/>One or more task names that precede the task being created in the current node. Task names are case sensitive, should not be quoted and must exist in the same schema in which the current task is being created. If there are multiple predecessor task separate the task names using a comma and no spaces.|
| **Root task name** | Visible when Task Schedule is set to Predecessor.<br/> Name of the root task that controls scheduling for the DAG of tasks. Task names are case sensitive, should not be quoted and must exist in the same schema in which the current task is being created. If there are multiple predecessor task separate the task names using a comma and no spaces.|

#### Delta Stream Advanced Scheduling Options

<img width="465" height="708" alt="image" src="https://github.com/user-attachments/assets/44c937fa-3827-4ab7-9480-8e23a8fb89e4" />

| **Option** | **Description** |
|------------|----------------|
| **Execute As Specific User** | Toggle to run on behalf of another user. Requires `GRANT IMPERSONATE` privileges. |
| **User Name** | The specific user account name used when **Execute As Specific User** is enabled. |
| **Allow Overlapping Execution** | Allows a new instance of the task to start if the previous one is still running. |
| **Enable Task Graph Config** | Enables a text box to provide **Configuration JSON** for the task graph. |
| **Auto-Suspend After Failures** | Automatically suspends the task after a set number of consecutive failures. |
| **Number of Consecutive Failures** | Set the threshold (0 - No Limit) before the task is automatically suspended. <br/>- When toggle is OFF: Parameter is not included (uses Snowflake default of 10).<br/>- When toggle is ON with value 0: **Disables** auto-suspension.<br/>- When toggle is ON with value > 0: **Suspends** after that many consecutive failures. |
| **Enable Auto-Retry** | Toggle to automatically retry the task if it fails. |
| **Retry Attempts** | Specify the number of retry attempts allowed (Range: 0 - 30). |

#### Delta Stream Notification Options

<img width="453" height="459" alt="image" src="https://github.com/user-attachments/assets/83b9dcfc-20c9-41c0-a459-a8706b3c885f" />

| **Option** | **Description** |
|------------|----------------|
| **Enable Error Notifications** | Toggle to send alerts on failure. Requires an **Error Integration Name**. |
| **Enable Success Notifications** | Toggle to send alerts on success. Requires a **Success Integration Name**. |

> **Note:** Options under **Advanced Scheduling Options** and **Notification Options** (Execution Time, Overlapping Execution, Auto-Suspend, Auto-Retry, etc.) are only applicable to **Root** and **Independent** tasks. The only exception is **Execute As Specific User**, which can be configured for any task in the graph.

### Delta Stream Limitations

> 🚧 **Appyling Transformation**
> This node can't apply transformations to the columns for this node type.

### Delta Stream Deployment

#### Delta Stream Parameters

It includes an environment parameter that allows you to specify a different warehouse to run a task in different environments.
The parameter name is `targetTaskWarehouse`, and the default value is `DEV ENVIRONMENT`.

When set to `DEV ENVIRONMENT`, the value entered in the Scheduling Options config "Select Warehouse on which to run the task" will be used when creating the task.

```json
{"targetTaskWarehouse": "DEV ENVIRONMENT"}
```

When set to any value other than `DEV ENVIRONMENT`, the node will attempt to create the task using a Snowflake warehouse with the specified value.
For example, with the setting below for the parameter in a QA environment, the task will execute using a warehouse named `compute_wh`.

```json
{"targetTaskWarehouse": "compute_wh"}
```

#### Delta Stream Initial Deployment

When deployed for the first time into an environment, executes the following stages:

| **Stage** | **Description** |
|-----------|----------------|
| **Create Stream** | Executes CREATE OR REPLACE statement to create Stream in target environment |
| **Create Target Table** | Creates destination table for processed data storage |
| **Target Table Initial Load** | Populates the target table with the existing data from the source object. This step ensures that the target table starts with a complete set of data before any further changes are applied |
| **Create Hybrid View** | Provides access to the most up-to-date data by combining the initial data load with any subsequent changes captured by the stream. The hybrid view ensures that users always have access to the latest version of the data without waiting for batch updates.|
| **Create Task** | Creates merge operation task for stream changes |
| **Resume Task** | Activates the created task for scheduled execution |
| **Apply Table Clustering** | Alters table with cluster key if enabled |
| **Resume Recluster Clustering** | Enables periodic reclustering to maintain optimal clustering |

##### Delta Stream Predecessor Task Deployment

When deploying with predecessor tasks, executes:

| **Stage** | **Description** |
|-----------|----------------|
| **Suspend Root Task** | Suspends root task to add task into DAG |
| **Create Task** | Creates task for loading target table |

> 📘 **Task DAG Note**
>
> For tasks in a DAG, include Task Dag Resume Root node type to resume root node after dependent task creation. Tasks use CREATE OR REPLACE only, with no ALTER capabilities.

##### Delta Stream Redeployment

| **Redeployment Behavior** | **Stage Executed** |
|--------------------------|-------------------|
| **Create Stream if not exists** | Re-Create Stream at existing offset |
| **Create or Replace** | Create Stream |
| **Create at existing stream** | Re-Create Stream at existing offset |

##### Delta Stream Table Redeployment

The following column/table changes trigger ALTER statements:

* Table name changes
* Column drops
* Column data type alterations
* New column additions

Executes these stages:

| **Stage** | **Description** |
|-----------|----------------|
| **Alter Table Operations** | Executes appropriate ALTER statements for schema changes |
| **Target Initial Load** | Executes load based on configuration:<br/>- If initial load enabled + "Create or Replace": Uses INSERT OVERWRITE<br/>- All other scenarios: Uses INSERT INTO |

##### Delta Stream View Redeployment

Stream or table changes trigger Hybrid View recreation.

##### Delta Stream Task Redeployment

Stream or table changes trigger:

1. Task recreation
2. Task resumption

> 🚧 Redeployment Behavior
>
> Redeployment with changes in Stream/Table/Task properties will result in execution of all steps mentioned in inital deployment.

#### Node Type Switching

Node Type switching is supported starting from Coalesce version **7.28+**.

From this version onward, a node’s materialization type can be switched from one supported type to another, subject to certain limitations.

For more info click here - [Node Type Switching Logic and Limitations](#node-type-switching-logic)

### Delta Stream Undeployment

When node is deleted, executes:

* Drop Stream
* Drop Table/Transient Table  
* Drop View
* Drop Current Task
* 
### Redeployment with no changes
 
If the nodes are redeployed with no changes compared to previous deployment, then no stages are executed

-----------------

#### Node Type Switching Logic
| Current MaterializationType | Desired MaterializationType | Stage |
|------------|--------|-------|
| SIM or DSM | Table | 1. Warning(if applicable)<br/> 2. Drop/Alter(depending on Create Target As config)<br/> 3. Create(if applicable) |
| Table | Table | 1. Warning (if applicable)<br/> 2. Drop<br/> 3. Create|
| Any other Task | Table | 1. Warning (if applicable)<br/> 2. Drop<br/> 3. Create |
| Any Other | Table | 1. Warning (if applicable)<br/>2. Drop <br/> 3. Create |

**Note:** SIM and DSM nodes contain a **Create Target As** configuration similar to **Deferred Merge - Append** and **Delta Stream**. When switching from SIM/DSM, the system performs an **Alter** if this configuration matches the desired state, or a **Drop and Create** if it differs. For all other task or table types, this configuration is absent and is treated as "blank" in the current state, triggering a mandatory **Drop and Create** to correctly initialize the Deferred Merge table properties.

Please review the documented limitations before performing a node type switch to ensure compatibility and avoid unintended deployment issues.

#### ⚠ Limitations of Node Type Switching (Current)

| # | Current Materialization | Desired Materialization | Limitation |
|---|--------------------------|--------------------------|------------|
| 1 | Older Version Iceberg Table | Table | Results in `ALTER` failure. Iceberg tables require `ALTER ICEBERG TABLE`. Works only if latest package (with switching support) is already used. |
| 2 | Older Version<br/>Create or Alter-View<br/>Data Quality-DMF | Any(except View) | Switch fails unless current node uses latest package supporting node type switching. |
| 3 | First Node in Pipeline | Any | Not supported. First node is foundational and switching may disrupt the pipeline. |
| 4 | External Packages | Any | Not supported as they typically act as first nodes in the pipeline. |
| 5 | Functional Packages | Any | Not supported due to column re-sync behavior which may cause schema inconsistencies. |
| 6 | Dynamic Dimension / LRV | Any | System columns must be manually dropped before redeployment. |
| 7 | Any | Any Other | After performing node switching, the `Create/Run` in Workspace browser may not work as expected due to changes in the node’s materialization type. |
| 8 | Table(Data Profiling) | Table | This may result in ALTER failure unless latest package is used(with system column removal support)**(Pending Release)** |
| 9 | Any | Any Stream-based Node (Stream, Stream & I/M, Delta Merge, or Directory Stream) | When switching to a Stream-based node, do not select **'Create At Existing Stream'** from the Redeployment Behavior; this causes deployment errors. Use **'Create or Replace'** or **'Create If Not Exists'**. |
| 10 | Stream | Stream for Directory Table (and vice versa) | Metadata columns are not automatically synchronized. Specific directory columns (e.g., `relative_path`, `size`, `md5`) are not added when switching to Directory Table, nor are they removed when switching back to a standard Stream. |
| 11 | Stream | Any Other (and vice versa) | Snowflake CDC metadata columns (`METADATA$ACTION`, `METADATA$ISUPDATE`, `METADATA$ROW_ID`) are not automatically managed. They are neither removed nor added when there's a node type switch |
| 12 | Deferred Merge - Append and Delta Stream | Any Other(and vice versa) | System columns are not automatically managed, They are neither removed nor added when there's a node type switch. These must be manually dropped or added before redeployment. |

--------------

## Code

### Deferred Merge - Append Stream

* [Node definition](https://github.com/coalesceio/Deferred-Merge/blob/main/nodeTypes/DeferredMerge-AppendStream-300/definition.yml)
* [Create Template](https://github.com/coalesceio/Deferred-Merge/blob/main/nodeTypes/DeferredMerge-AppendStream-300/create.sql.j2)

### Deferred Merge - Delta Stream 

* [Node definition](https://github.com/coalesceio/Deferred-Merge/blob/main/nodeTypes/DeferredMerge-DeltaStream-301/definition.yml)
* [Create Template](https://github.com/coalesceio/Deferred-Merge/blob/main/nodeTypes/DeferredMerge-DeltaStream-301/create.sql.j2)
