import Page from "./Page";
import TableRow from "./components/TableRow";
import ClusterDetails from "./components/ClusterDetails";

export default class Cluster extends Page {
  get url() {
    return "/api/v3/clusters";
  }

  get dataKey() {
    return "clusterList";
  }

  get formFields() {
    return [
      { label: "Name", name: "name", value: "", type: "input" },
      {
        label: "Status",
        name: "status",
        value: "",
        type: "option",
        optionValues: ["", "UP", "OUT_OF_SERVICE", "TERMINATED"]
      },
      { label: "Tag", name: "tag", value: "", type: "input" },
      {
        label: "Sort By",
        name: "sort",
        value: "",
        type: "select",
        selectFields: ["name", "status", "tag"].map(field => ({
          value: field,
          label: field
        }))
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

  get searchPath() {
    return "clusters";
  }

  get rowType() {
    return TableRow;
  }

  get tableHeader() {
    return [
      "Id",
      "Name",
      "Copy Link",
      "User",
      "Status",
      "Version",
      "Tags",
      "Created (UTC)",
      "Updated (UTC)"
    ];
  }

  get detailsTable() {
    return ClusterDetails;
  }
}
