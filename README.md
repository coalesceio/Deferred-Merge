# Deferred Merge Append Stream

The Deferred Merge - Append Stream Node includes several key steps to ensure efficient and up-to-date data processing.
First, a stream is created to capture changes from the source object. Then a target table is created and initially loaded with data.
A hybrid view is established to provide access to the most current data by combining initial and incremental updates.

Finally, a scheduled task manages ongoing updates by merging changes into the target table. This process ensures that the target table remains synchronized with the source data  maintaining accuracy and timeliness.

* [Deferred Merge Append Stream](#deferred-merge-append-stream)
  * [Node Configuration](#node-configuration)
    * [General Options](#general-options)
    * [Stream Options](#stream-options)
      * [Table](#table)
    * [Target Loading Options](#target-loading-options)
    * [Target Row DML Operations](#target-row-dml-operations)
    * [Target Delete Options](#target-delete-options)
    * [Target Clustering Options](#target-clustering-options)
    * [Scheduling Options](#scheduling-options)
  * [Limitations](#limitations)
  * [Deployment](#deployment)
    * [Initial Deployment](#initial-deployment)
      * [Predecessor Task](#predecessor-task)
    * [Redeployment](#redeployment)
      * [Create Stream If Not Exists](#create-stream-if-not-exists)
      * [Create or Replace](#create-or-replace)
      * [Create at Existing Stream](#create-at-existing-stream)
    * [Undeployment](#undeployment)
  * [Code](#code)
    * [Deffered Merge - Append Stream](#deffered-merge---append-stream)

## Node Configuration

The Stream has multiple configuration groups.

### General Options

* **Development Mode**: True / False toggle that determines whether a task will be created or if the SQL to be used in the task will execute as DML as a Run action. Prior to creating a task, it is helpful to test the SQL the task will execute to make sure it runs without errors and returns the expected data.
  * True - A table will be created and SQL will execute as a Run action.
  * False - After sufficiently testing the SQL as a Run action setting Development Mode to false will wrap the SQL statement in a task with options specified in Scheduling Options.

### Stream Options

There are two configs within the Stream Options group.

* **Source Object**: The type of object the stream will be created on. One of two options is required to be selected and this selection drives what other configs are available:
  * Table
  * View

#### Table

* **Show Initial Rows**: True / False Toggle to specify the records to return the first time the stream is consumed.
  * True - The stream returns only the rows that existed in the source object at the moment when the stream was created. Subsequently, the stream returns any DML changes to the source object since the most recent offset; that is, the normal stream behavior.
  * False - The stream returns any DML changes to the source object since the most recent offset.
* **Show Initial Rows**: True / False Toggle to specify the records to return the first time the stream is consumed.
  * True - The stream returns only the rows that existed in the source object at the moment when the stream was created. Subsequently, the stream returns any DML changes to the source object since the most recent offset; that is, the normal stream behavior.
  * False - The stream returns any DML changes to the source object since the most recent offset.

### Target Loading Options

* **Table keys**: The business keys columns based on which the data is merged into Target table
* **Record Versioning** : Allows to add Date Time or Date and Timestamp column based on which latest record is merged into Target table.

### Target Row DML Operations

* **Column Identifier**: Allows to add the column that identifies DML Operations.
* **Include Value for Update**: For records flagged under Update, the existing records in the target table are updated with the corresponding values from the source table.
* **Insert Value**:  It indicates that the corresponding record is meant to be inserted into the target table. This condition ensures that only records flagged for insertion are actually inserted into the target table during the merge operation.

* **Delete Value**: This value indicates that the corresponding record should either be soft-deleted (if the condition is met by enabling the soft delete toggle) or hard-deleted from the target table.

### Target Delete Options

* **Soft Delete**: Enabling this toggle maintains a record of deleted data for auditing purposes.
* **Retain Last Non-Deleted Values**: Preserves the most recent non-deleted record in a dataset, even as other records are marked as deleted or become inactive.

### Target Clustering Options

* **Cluster key**: True/False to determine whether the target table is to be clustered or not
  * True - Allows you to specify the column based on which clustering is to be done.
    * Allow Expressions Cluster Key-True ->allows to add an expression to the specified cluster key
  * False – No clustering done.

### Scheduling Options

If development mode is set to false then Scheduling Options can be used to configure how and when the task will run.
There are multiple options that can be selected depending on combinations of configs that are selected:

* **Scheduling Mode**: Specifies whether a warehouse or serverless compute is used to run the task.
  * Warehouse Task - User managed warehouse will execute tasks.
  * Serverless Task - Utilize serverless compute to execute tasks.
* **When Source Stream has Data Flag**: True / False toggle that checks whether source streams have data before executing a task.
  * True - Only run task if source stream has capture change data.
  * False - Run tasks on schedule regardless of whether the source stream has data. If the source is not a stream should set this to false.
* **Select Warehouse on which to run task**: Visible if Scheduling Mode is set to Warehouse Task
  * Enter the name of the warehouse you want the task to run on without quotes
* **Select initial serverless Warehouse size**: Visible when Scheduling Mode is set to Serverless Task
  * Select the initial compute size on which to run the task. Snowflake will adjust size from there based on target schedule and task run times.
* **Task Schedule**: Select how you want to schedule the task to run.
  * **Minutes** - Allows you to specify a minute interval for running task.
  * **Cron** - Allows you to specify a CRON schedule for running task.
  * **Predecessor** - Allows you to specify a predecessor task to determine when a task should execute.
* **Enter task schedule using minutes**: Only visible when Task Schedule is set to Minutes. Enter a whole number from 1 to 11520 which represents the number of minutes between task runs.
* **Enter task schedule using Cron**: Only visible when Task Schedule is set to Cron. Specifies a cron expression and time zone for periodically running the task. Supports a subset of standard cron utility syntax.
* **Enter predecessor tasks separated by a comma**: Only visible when Task Schedule is set to Predecessor. One or more task names that precede the task being created in the current node. Task names are case sensitive, should not be quoted and must exist in the same schema in which the current task is being created. If there are multiple predecessor tasks separate the task names using a comma and no spaces.
* **Enter root task name**: Name of the root task that controls scheduling for the DAG of tasks. Task names are case sensitive, should not be quoted and must exist in the same schema in which the current task is being created. If there are multiple predecessor tasks separate the task names using a comma and no spaces.

## Limitations

> 🚧 **Appyling Transformation**
>
> This node can't apply transformations to the columns for this node type.

## Deployment

### Initial Deployment

When deployed for the first time into an environment, the following stages will be executed.

1. **Create Stream**: This stage will execute a CREATE OR REPLACE statement and create a Stream in the target environment.
2. **Create Target Table**: This table is the destination where the processed data will be stored.
3. **Target Table Initial Load**: This involves populating the target table with the existing data from the source object. This step ensures that the target table starts with a complete set of data before any further changes are applied.
4. **Create Hybrid View**: This view provides access to the most up-to-date data by combining the initial data load with any subsequent changes captured by the stream. The hybrid view ensures that users  always have access to the latest version of the data without waiting for batch updates.
5. **Create Task**: This task wraps a merge operation that incorporates the changes captured by the stream into the target table.
6. **Resume Task**: After the task has been created it needs to be resume so that the task runs on the schedule
7. **Apply Table Clustering**: This Step will alter a cluster Key to the table if the cluster toggle is enabled.
8. **Resume Recluster Clustering**: As DML operations  are performed on a clustered table, the data in the table might become less clustered. Periodic/regular reclustering of the table is required to maintain optimal clustering.

#### Predecessor Task

* **Suspend Root Task**: To add a task into a DAG of task the root task needs to be put into a suspended state.
* **Create Task**: This stage will create a task that will load the target table on the schedule specified. 
  * If a task is part of a DAG of tasks the DAG needs to include a node type called Task Dag Resume Root. 
  * This node will resume the root node once all the dependent tasks have been created as part of a deployment. The task node has no ALTER capabilities. All task enabled nodes are CREATE OR REPLACE only though this is subject to change.

### Redeployment

After the Object has deployed for the first time into a target environment, subsequent deployments will result in a new stream creation based on redeployment behavior chosen.

#### Create Stream If Not Exists

The following stage is executed:

* Re-Create Stream at existing offset

#### Create or Replace

The following stage is executed:

* Create Stream

#### Create at Existing Stream

The following stage is executed:

* Re-Create Stream at existing offset

### Undeployment

If a object is deleted from a Workspace, that Workspace is committed to Git and that commit deployed to a higher level environment then the Stream in the target environment will be dropped.

This is executed in following stages:

* Drop Current Task
* Drop Stream
* Drop Table
* Drop View
  
## Code

### Deffered Merge - Append Stream

* [Node definition](https://github.com/coalesceio/Deferred-Merge/blob/main/nodeTypes/DeferredMerge-AppendStream-293/definition.yml)
* [Create Template](https://github.com/coalesceio/Deferred-Merge/blob/main/nodeTypes/DeferredMerge-AppendStream-293/create.sql.j2)
* [Run Template](https://github.com/coalesceio/Deferred-Merge/blob/main/nodeTypes/DeferredMerge-AppendStream-293/run.sql.j2)

[<img src="https://github.com/coalesceio/Deferred-Merge/assets/7216836/352cf505-cd1c-4b25-b61c-6e028c7df3ee" alt="Git Logo" height="40">](https://github.com/coalesceio/Deferred-Merge)
