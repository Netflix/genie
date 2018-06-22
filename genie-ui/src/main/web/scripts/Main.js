import T from "prop-types";
import React from "react";
import {fetch} from "./utils";

import SiteHeader from "./components/SiteHeader";
import SiteFooter from "./components/SiteFooter";
import AuthCheck from "./components/AuthCheck";

export default class App extends React.Component {
  static propTypes = { headers: T.array, children: T.element.isRequired };

  static defaultProps = {
    headers: [
      { url: "/jobs", name: "GENIE", className: "supress" },
      { url: "/jobs", name: "Jobs", className: "active" },
      { url: "/clusters", name: "Clusters", className: "active" },
      { url: "/commands", name: "Commands", className: "active" },
      { url: "/applications", name: "Applications", className: "active" }
    ]
  };

  constructor(props) {
    super(props);
    this.state = {
      version: "",
      infos: [
        { className: "supress", name: "", url: "#" }
      ]
    };
  }

  componentDidMount() {
    fetch("/admin/info", null, "GET", "application/json").done(data => {
      this.setState({ version: data.genie.version });
    });
      fetch("/ui/user", null, "GET", "application/json").done(
          data => {
              this.setState(
                  {
                      infos: [
                          {
                              className: "supress",
                              name: data.name,
                              url: "#"
                          }
                      ]
                  }
              );
          }
      );
  }

  render() {
    return (
      <div>
        <SiteHeader headers={this.props.headers} infos={this.state.infos} />
        <AuthCheck />
        <div className="container">
          {this.props.children}
        </div>
        <SiteFooter version={this.state.version} />
      </div>
    );
  }
}
