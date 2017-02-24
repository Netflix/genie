import Page from "./Page";
import TableRow from "./components/TableRow";
import CommandDetails from "./components/CommandDetails";

export default class Command extends Page {
  get url() {
    return "/api/v3/commands";
  }

  get dataKey() {
    return "commandList";
  }

  get formFields() {
    return [
      { label: "Name", name: "name", value: "", type: "input" },
      { label: "User", name: "user", value: "", type: "input" },
      {
        label: "Status",
        name: "status",
        value: "",
        type: "option",
        optionValues: ["", "ACTIVE", "DEPRECATED", "INACTIVE"]
      },
      { label: "Tag", name: "tag", value: "", type: "input" },
      {
        label: "Sort By",
        name: "sort",
        value: "",
        type: "select",
        selectFields: ["name", "user", "status", "tag"].map(field => ({
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
    return "commands";
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
    return CommandDetails;
  }
}
