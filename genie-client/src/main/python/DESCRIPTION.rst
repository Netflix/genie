
===================
Genie Python Client
===================

This package provides a robust client for interacting with an existing Genie service. Included are modules for the
resource models (Application, Command, Cluster, Job, etc), exceptions and retry logic wrappers for API calls.

For more documentation on Genie and its available API's see the `Genie GitHub <http://netflix.github.io/genie/>`_ page.

Examples
--------

Configuration Service Example
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This example shows how to register a new application with a running Genie instance. Similar processes can be followed
for registering clusters and commands as well as relating all three to each other.

::

    import genie2.client.wrapper
    import genie2.model.Application

    # Create a Genie client which proxies API calls through wrapper which retries failures based on various return codes
    genie = genie2.client.wrapper.Genie2("http://localhost:7001/genie", genie2.client.wrapper.RetryPolicy(tries=8, none_on_404=True, no_retry_http_codes=range(400, 500)))

    # Create a new application instance and set required fields
    app = genie2.model.Application.Application()
    app.name = "exampleAppName"
    app.user = "exampleUser"
    app.version = "0.0.1"
    app.status = "ACTIVE"

    # Save the application to the service
    created_app = genie.createApplication(app)
    print created_app.id

    # Retrieve the application by ID
    got_app = genie.getApplication(created_app.id)
    print got_app.name

    # Delete the application by ID
    deleted_app = genie.deleteApplication(got_app.id)
    print deleted_app.id


Execution Service Example
~~~~~~~~~~~~~~~~~~~~~~~~~

This example shows how to execute a job on Genie. In this case it's running a `Presto <http://prestodb.io/>`_ query.
This assumes the Presto cluster has already been configured with Genie and the command registered.

::

    import genie2.client.wrapper
    import genie2.model.Job
    import genie2.model.ClusterCriteria

    # Create a Genie client which proxies API calls through wrapper which retries failures based on various return codes
    genie = genie2.client.wrapper.Genie2("http://localhost:7001/genie", genie2.client.wrapper.RetryPolicy(tries=8, none_on_404=True, no_retry_http_codes=range(400, 500)))

    # Create a job instance and fill in the required parameters
    job = genie2.model.Job.Job()
    job.name = "GeniePythonClientExampleJob"
    job.user = "tgianos"
    job.version = "0.0.1"

    # Create a list of cluster criterias which determine the cluster to run the job on
    cluster_criterias = list()
    cluster_criteria = genie2.model.ClusterCriteria.ClusterCriteria()
    criteria = set()
    criteria.add("presto")
    criteria.add("prod")
    cluster_criteria.tags = criteria
    cluster_criterias.append(cluster_criteria)
    job.clusterCriterias = cluster_criterias

    # Create the set of command criteria which will determine what command Genie executes for the job
    command_criteria = set()
    command_criteria.add("presto")
    job.commandCriteria = command_criteria

    # Any command line arguments to run along with the command. In this case it holds the actual query but this
    # could also be done via an attachment or file dependency.
    job.commandArgs = "--execute \"show tables;\""

    # Submit the job to Genie
    running_job = genie.submitJob(job)

    # Check on the status of the job
    job_status = genie.getJobStatus(running_job.id)
    print job_status


