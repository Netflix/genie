#!/usr/bin/env bash

set -o errexit -o nounset -o pipefail

function print_help() {
    echo "Usage: import-to-elasticsearch-from-2.0.0.sh [OPTIONS]"
    echo "OPTIONS (Space delimited ex: --parameter value):"
    printf "%-40s %-80s %s\n" "Parameter Name" "Description" "Default Value"
    printf "%-40s %-80s %s\n" "mysql_host" "The hostname of MySQL server." "localhost"
    printf "%-40s %-80s %s\n" "mysql_port" "The port of the MySQL server." "3306"
    printf "%-40s %-80s %s\n" "mysql_user" "The user of MySQL." "root"
    printf "%-40s %-80s %s\n" "mysql_password" "The password for the MySQL user." ""
    printf "%-40s %-80s %s\n" "mysql_database" "The name of the mysql database to use." "genie"
    printf "%-40s %-80s %s\n" "elasticsearch_version" "The version of elasticsearch to index into. OPTIONS: [1.7.2|1.7.0|1.6.0|1.5.2]" "1.7.2"
    printf "%-40s %-80s %s\n" "elasticsearch_protocol" "Whether https should be enabled. OPTIONS: [http|https]" "http"
    printf "%-40s %-80s %s\n" "elasticsearch_host" "The hostname of the elasticsearch node to index into." "localhost"
    printf "%-40s %-80s %s\n" "elasticsearch_http_port" "The http port the elasticsearch cluster is listening on." "9200"
    printf "%-40s %-80s %s\n" "elasticsearch_transport_port" "The transport port elasticsearch cluster is listening on." "9300"
    printf "%-40s %-80s %s\n" "elasticsearch_cluster_name" "The name of your elastic search cluster." "elasticsearch"
    printf "%-40s %-80s %s\n" "elasticsearch_index" "The elastic search index to write the documents into." "genie"
    printf "%-40s %-80s %s\n" "help" "Print this help message"
    exit 0
}

MYSQL_HOST="localhost"
MYSQL_PORT="3306"
MYSQL_USER="root"
MYSQL_PASSWORD=""
MYSQL_DATABASE="genie"
ELASTICSEARCH_VERSION="1.7.2"
ELASTICSEARCH_PROTOCOL="http"
ELASTICSEARCH_HOST="localhost"
ELASTICSEARCH_HTTP_PORT="9200"
ELASTICSEARCH_TRANSPORT_PORT="9300"
ELASTICSEARCH_CLUSTER_NAME="elasticsearch"
ELASTICSEARCH_INDEX_NAME="genie"

# Use > 1 to consume two arguments per pass in the loop (e.g. each
# argument has a corresponding value to go with it).
# Use > 0 to consume one or more arguments per pass in the loop (e.g.
# some arguments don't have a corresponding value to go with it such
# as in the --help example).
while [[ $# > 0 ]]
do
KEY="$1"

case ${KEY} in
    --mysql_host)
        MYSQL_HOST="$2"
        shift # past argument
        ;;
    --mysql_port)
        MYSQL_PORT="$2"
        shift # past argument
        ;;
    --mysql_user)
        MYSQL_USER="$2"
        shift # past argument
        ;;
    --mysql_password)
        MYSQL_PASSWORD="$2"
        shift # past argument
        ;;
    --mysql_database)
        MYSQL_DATABASE="$2"
        shift # past argument
        ;;
    --elasticsearch_version)
        ELASTICSEARCH_VERSION="$2"
        shift # past argument
        ;;
    --elasticsearch_protocol)
        ELASTICSEARCH_PROTOCOL="$2"
        shift # past argument
        ;;
    --elasticsearch_host)
        ELASTICSEARCH_HOST="$2"
        shift # past argument
        ;;
    --elasticsearch_http_port)
        ELASTICSEARCH_HTTP_PORT="$2"
        shift # past argument
        ;;
    --elasticsearch_transport_port)
        ELASTICSEARCH_HTTP_PORT="$2"
        shift # past argument
        ;;
    --elasticsearch_cluster_name)
        ELASTICSEARCH_CLUSTER_NAME="$2"
        shift # past argument
        ;;
    --elasticsearch_index)
        ELASTICSEARCH_INDEX_NAME="$2"
        shift # past argument
        ;;
    --help)
        print_help
        ;;
    *)
        echo "Unknown option"
        print_help
        ;;
esac
shift # past argument or value
done

# Use /tmp as working directory
pushd "/tmp" > /dev/null

# Based on https://github.com/jprante/elasticsearch-jdbc#recent-versions
if [ "${ELASTICSEARCH_VERSION}" == "1.7.2" ]; then
    ELASTICSEARCH_JDBC_VERSION="1.7.2.1"
elif [ "${ELASTICSEARCH_VERSION}" == "1.7.0" ]; then
    ELASTICSEARCH_JDBC_VERSION="1.7.0.1"
elif [ "${ELASTICSEARCH_VERSION}" == "1.6.0" ]; then
    ELASTICSEARCH_JDBC_VERSION="1.6.0.1"
elif [ "${ELASTICSEARCH_VERSION}" == "1.5.2" ]; then
    ELASTICSEARCH_JDBC_VERSION="1.5.2.0"
else
    echo "Unsupported Elasticsearch version: ${ELASTICSEARCH_VERSION}."
    popd > /dev/null
    print_help
fi

# Based on https://github.com/jprante/elasticsearch-jdbc#recent-versions
if [ "${ELASTICSEARCH_PROTOCOL}" != "http" ] && [ "${ELASTICSEARCH_PROTOCOL}" != "https" ]; then
    echo "Unsupported Elasticsearch protocol: ${ELASTICSEARCH_PROTOCOL}."
    popd > /dev/null
    print_help
fi

printf "%-40s %-50s\n" "Variable" "Value"
printf "%-40s %-50s\n" "mysql_host" "${MYSQL_HOST}"
printf "%-40s %-50s\n" "mysql_port" "${MYSQL_PORT}"
printf "%-40s %-50s\n" "mysql_user" "${MYSQL_USER}"
printf "%-40s %-50s\n" "mysql_password" "${MYSQL_PASSWORD}"
printf "%-40s %-50s\n" "mysql_database" "${MYSQL_DATABASE}"
printf "%-40s %-50s\n" "elasticsearch_version" "${ELASTICSEARCH_VERSION}"
printf "%-40s %-50s\n" "elasticsearch_protocol" "${ELASTICSEARCH_PROTOCOL}"
printf "%-40s %-50s\n" "elasticsearch_host" "${ELASTICSEARCH_HOST}"
printf "%-40s %-50s\n" "elasticsearch_http_port" "${ELASTICSEARCH_HTTP_PORT}"
printf "%-40s %-50s\n" "elasticsearch_transport_port" "${ELASTICSEARCH_TRANSPORT_PORT}"
printf "%-40s %-50s\n" "elasticsearch_cluster_name" "${ELASTICSEARCH_CLUSTER_NAME}"
printf "%-40s %-50s\n" "elasticsearch_index" "${ELASTICSEARCH_INDEX_NAME}"

echo "Variables ok? (y/n): "
read ANSWER

if [ "${ANSWER}" != "y" ]; then
    echo "Bye!"
    exit 0
fi

# Download the appropriate elasticsearch-jdbc version into /tmp if it isn't already there
if [ ! -f "elasticsearch-jdbc-${ELASTICSEARCH_JDBC_VERSION}-dist.zip" ]; then
    wget "http://xbib.org/repository/org/xbib/elasticsearch/importer/elasticsearch-jdbc/${ELASTICSEARCH_JDBC_VERSION}/elasticsearch-jdbc-${ELASTICSEARCH_JDBC_VERSION}-dist.zip"
fi

if [ ! -d "elasticsearch-jdbc-${ELASTICSEARCH_JDBC_VERSION}" ]; then
    unzip "elasticsearch-jdbc-${ELASTICSEARCH_JDBC_VERSION}-dist.zip"
fi

cd "elasticsearch-jdbc-${ELASTICSEARCH_JDBC_VERSION}"

# See if the desired index already exists
if [ "$(curl -X HEAD -i -s -o /dev/null -w "%{http_code}" "${ELASTICSEARCH_PROTOCOL}://${ELASTICSEARCH_HOST}:${ELASTICSEARCH_HTTP_PORT}/${ELASTICSEARCH_INDEX_NAME}")" -ne "200" ]; then
    if [ "$(curl -X PUT -i -s -o /dev/null -w "%{http_code}" "${ELASTICSEARCH_PROTOCOL}://${ELASTICSEARCH_HOST}:${ELASTICSEARCH_HTTP_PORT}/${ELASTICSEARCH_INDEX_NAME}")" -ne "200" ]; then
        echo "Unable to create index ${ELASTICSEARCH_INDEX_NAME} in Elasticsearch. Exiting."
        popd > /dev/null
        exit 1
    fi
fi

# "sql" : "SELECT Job.id, Job.created, Job.updated, Job.name, Job.user, Job.version, Job.description, Job.archiveLocation, Job.chosenClusterCriteriaString, Job.clusterCriteriasString, Job.commandCriteriaString, Job.commandId, Job.email, Job.envPropFile as 'setupFile', Job.executionClusterId, Job.exitCode, Job.fileDependencies, Job.finished, Job.groupName as 'group', Job.hostName, Job.killURI, Job.outputURI, Job.started, Job.status, Job.statusMsg, Job_tags.element as 'tags', '${ELASTICSEARCH_INDEX_NAME}' as _index, 'job' as _type, Job.id as _id, Job.entityVersion as _version FROM Job LEFT JOIN Job_tags ON Job.id = Job_tags.JOB_ID ORDER BY _id",
# "sql" : "select *, '${ELASTICSEARCH_INDEX_NAME}' as _index, 'job' as _type, id as _id from Job",
echo "
{
    \"type\": \"jdbc\",
    \"jdbc\": {
        \"url\": \"jdbc:mysql://${MYSQL_HOST}:${MYSQL_PORT}/${MYSQL_DATABASE}\",
        \"user\": \"${MYSQL_USER}\",
        \"password\": \"${MYSQL_PASSWORD}\",
        \"sql\": [
            {
                \"statement\": \"SELECT Job.id, Job.created, Job.updated, Job.name, Job.user, Job.version, Job.description, Job.archiveLocation, Job.chosenClusterCriteriaString, Job.clusterCriteriasString, Job.commandCriteriaString, Job.commandId, Job.email, Job.envPropFile AS 'setupFile', Job.executionClusterId, Job.exitCode, Job.fileDependencies, Job.finished, Job.groupName AS 'group', Job.hostName, Job.killURI, Job.outputURI, Job.started, Job.status, Job.statusMsg, Job_tags.element AS 'tags', Job.id as _id, Job.entityVersion AS _version FROM Job LEFT JOIN Job_tags ON Job.id = Job_tags.JOB_ID ORDER BY _id\"
            }
        ],
        \"elasticsearch\": {
             \"cluster\": \"${ELASTICSEARCH_CLUSTER_NAME}\",
             \"host\": \"${ELASTICSEARCH_HOST}\",
             \"port\": ${ELASTICSEARCH_TRANSPORT_PORT}
        },
        \"index\": \"${ELASTICSEARCH_INDEX_NAME}\",
        \"type\": \"job\",
        \"detect_json\": false,
        \"type_mapping\": {
            \"job\": {
                \"properties\": {
                    \"id\": {
                        \"type\": \"string\",
                        \"index\": \"not_analyzed\"
                    },
                    \"name\": {
                        \"type\": \"string\",
                        \"index\": \"not_analyzed\"
                    },
                    \"user\": {
                        \"type\": \"string\",
                        \"index\": \"not_analyzed\"
                    },
                    \"version\": {
                        \"type\": \"string\",
                        \"index\": \"not_analyzed\"
                    },
                    \"archiveLocation\": {
                        \"type\": \"string\",
                        \"index\": \"not_analyzed\"
                    },
                    \"commandId\": {
                        \"type\": \"string\",
                        \"index\": \"not_analyzed\"
                    },
                    \"executionClusterId\": {
                        \"type\": \"string\",
                        \"index\": \"not_analyzed\"
                    },
                    \"tags\": {
                        \"type\": \"string\",
                        \"index\": \"not_analyzed\"
                    }
                }
            }
        }
    }
}
" | java -cp "lib/*" -Dlog4j.configurationFile=bin/log4j2.xml org.xbib.tools.Runner org.xbib.tools.JDBCImporter

popd > /dev/null
exit 0