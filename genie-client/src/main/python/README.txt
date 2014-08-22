===========
Genie Python Client
===========

Typical usage often looks like this::

    #!/usr/bin/env python

    from genie_client.apis import cluster_api
  
    cluster_id = "blah" 
    service_base_url = "http://localhost:7001"
    cluster_api.Cluster(service_base_url)
    cluster = cluster_api.get_cluster(cluster_id) 


(Note the double-colon and 4-space indent formatting above.)

Cluster Api
===========

Methods supported by Cluster Api:

* get_cluster 

* get_clusters 

Samples
-------------

Examples to use cluster API:

1. TODO example 1 

2. TODO example 2 

Go to Genie OSS page at `<https://github.com/Netflix/genie/>`_.
