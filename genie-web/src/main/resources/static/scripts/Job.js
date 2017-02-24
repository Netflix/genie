import moment from "moment";
import Page from "./Page";
import TableRow from "./components/JobTableRow";
import JobDetails from "./components/JobDetails";

export default class Job extends Page {
  get url() {
    return "/api/v3/jobs";
  }

  get dataKey() {
    return "jobSearchResultList";
  }

  get formFields() {
    return [
      { label: "User Name", name: "user", value: "", type: "input" },
      { label: "Job Id", name: "id", value: "", type: "input" },
      { label: "Job Name", name: "name", value: "", type: "input" },
      {
        label: "Status",
        name: "status",
        value: "",
        type: "select",
        selectFields: [
          "INIT",
          "RUNNING",
          "SUCCEEDED",
          "FAILED",
          "KILLED",
          "INVALID"
        ].map(field => ({ value: field, label: field }))
      },
      {
        label: "Start Range",
        name: "startTime",
        value: [],
        type: "timeRange",
        queryMapping: ["minStarted", "maxStarted"],
        mapper: x => moment(parseInt(x, 10)).utc()
      },
      {
        label: "Finished Range",
        name: "finishedTime",
        value: [],
        type: "timeRange",
        queryMapping: ["minFinished", "maxFinished"],
        mapper: x => moment(parseInt(x, 10)).utc()
      },
      {
        label: "Size",
        name: "size",
        value: 25,
        type: "option",
        optionValues: [10, 25, 50, 100]
      },
      {
        label: "Sort By",
        name: "sort",
        value: "",
        type: "select",
        selectFields: [
          "user",
          "id",
          "name",
          "status",
          "clusterName",
          "clusterId",
          "created",
          "started",
          "finished"
        ].map(field => ({ value: field, label: field }))
      },
      {
        label: "Order",
        name: "sortOrder",
        value: "desc",
        type: "sortOption",
        optionValues: ["desc", "asc"]
      }
    ];
  }

  get hiddenFormFields() {
    return [
      { label: "Tags", name: "tag", value: "", type: "input" },
      { label: "Cluster Name", name: "clusterName", value: "", type: "input" },
      { label: "Cluster ID", name: "clusterId", value: "", type: "input" },
      { label: "Command Name", name: "commandName", value: "", type: "input" },
      { label: "Command ID", name: "commandId", value: "", type: "input" }
    ];
  }

  get searchPath() {
    return "jobs";
  }

  get rowType() {
    return TableRow;
  }

  get tableHeader() {
    return [
      "Job Id",
      "Name",
      "Output",
      "Copy Link",
      "User",
      "Status",
      "Cluster",
      "Started (UTC)",
      "Finished (UTC)",
      "Run Time"
    ];
  }

  get detailsTable() {
    return JobDetails;
  }
}
