# Deferred Merge Package

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
| **Development Mode** | True/False toggle that determines task creation:<br/>- **True**: Table created and SQL executes as Run action<br/>- **False**: SQL wraps into task with Scheduling Options |
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

![image](https://github.com/coalesceio/Deferred-Merge/assets/169126315/08023b93-9506-447b-87e8-088696e356ba)

| **Option** | **Description** |
|------------|----------------|
| **Scheduling Mode** | Choose compute type:<br/>- **Warehouse Task**: User managed warehouse executes tasks<br/>- **Serverless Task**: Uses serverless compute |
| **When Source Stream has Data Flag** | True/False toggle to check for stream data<br/>**True** - Only run task if source stream has capture change data<br/>**False** -  Run task on schedule regardless of whether the source stream has data. If the source is not a stream should set this to false. |
| **Select Warehouse** | Visible if Scheduling Mode is set to Warehouse Task. Enter the name of the warehouse you want the task to run on without quotes.|
| **Select initial serverless size** | Visible when Scheduling Mode is set to Serverless Task.<br/> Select the initial compute size on which to run the task. Snowflake will adjust size from there based on target schedule and task run times. |
| **Task Schedule** | Choose schedule type:<br/>- **Minutes** - Specify interval in minutes. Enter a whole number from 1 to 11520 which represents the number of minutes between task runs.<br/>- **Cron** - Uses [Cron expressions](https://docs.coalesce.io/docs/reference/cron-reference/). Specifies a cron expression and time zone for periodically running the task. Supports a subset of standard cron utility syntax.<br/>- **Predecessor** - Specify dependent tasks |
| **Enter predecessor tasks separated by a comma**| Visible when Task Schedule is set to Predecessor. <br/>One or more task names that precede the task being created in the current node. Task names are case sensitive, should not be quoted and must exist in the same schema in which the current task is being created. If there are multiple predecessor task separate the task names using a comma and no spaces.|
| **Root task name** | Visible when Task Schedule is set to Predecessor.<br/> Name of the root task that controls scheduling for the DAG of tasks. Task names are case sensitive, should not be quoted and must exist in the same schema in which the current task is being created. If there are multiple predecessor task separate the task names using a comma and no spaces.|

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
| **Development Mode** | True/False toggle that determines task creation:<br/>- **True**: Table created and SQL executes as Run action<br/>- **False**: SQL wraps into task with Scheduling Options |
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

![image](https://github.com/coalesceio/Deferred-Merge/assets/169126315/08023b93-9506-447b-87e8-088696e356ba)

| **Option** | **Description** |
|------------|----------------|
| **Scheduling Mode** | Choose compute type:<br/>- **Warehouse Task**: User managed warehouse executes tasks<br/>- **Serverless Task**: Uses serverless compute |
| **When Source Stream has Data Flag** | True/False toggle to check for stream data<br/>**True** - Only run task if source stream has capture change data<br/>**False** -  Run task on schedule regardless of whether the source stream has data. If the source is not a stream should set this to false. |
| **Select Warehouse** | Visible if Scheduling Mode is set to Warehouse Task. Enter the name of the warehouse you want the task to run on without quotes.|
| **Select initial serverless size** | Visible when Scheduling Mode is set to Serverless Task.<br/> Select the initial compute size on which to run the task. Snowflake will adjust size from there based on target schedule and task run times. |
| **Task Schedule** | Choose schedule type:<br/>- **Minutes** - Specify interval in minutes. Enter a whole number from 1 to 11520 which represents the number of minutes between task runs.<br/>- **Cron** - Uses [Cron expressions](https://docs.coalesce.io/docs/reference/cron-reference/). Specifies a cron expression and time zone for periodically running the task. Supports a subset of standard cron utility syntax.<br/>- **Predecessor** - Specify dependent tasks |
| **Enter predecessor tasks separated by a comma**| Visible when Task Schedule is set to Predecessor. <br/>One or more task names that precede the task being created in the current node. Task names are case sensitive, should not be quoted and must exist in the same schema in which the current task is being created. If there are multiple predecessor task separate the task names using a comma and no spaces.|
| **Root task name** | Visible when Task Schedule is set to Predecessor.<br/> Name of the root task that controls scheduling for the DAG of tasks. Task names are case sensitive, should not be quoted and must exist in the same schema in which the current task is being created. If there are multiple predecessor task separate the task names using a comma and no spaces.|

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

### Delta Stream Undeployment

When node is deleted, executes:

* Drop Stream
* Drop Table/Transient Table  
* Drop View
* Drop Current Task
* 
### Redeployment with no changes
 
If the nodes are redeployed with no changes compared to previous deployment, then no stages are executed

## Code

### Deferred Merge - Append Stream

* [Node definition](https://github.com/coalesceio/Deferred-Merge/blob/main/nodeTypes/DeferredMerge-AppendStream-300/definition.yml)
* [Create Template](https://github.com/coalesceio/Deferred-Merge/blob/main/nodeTypes/DeferredMerge-AppendStream-300/create.sql.j2)

### Deferred Merge - Delta Stream 

* [Node definition](https://github.com/coalesceio/Deferred-Merge/blob/main/nodeTypes/DeferredMerge-DeltaStream-301/definition.yml)
* [Create Template](https://github.com/coalesceio/Deferred-Merge/blob/main/nodeTypes/DeferredMerge-DeltaStream-301/create.sql.j2)
