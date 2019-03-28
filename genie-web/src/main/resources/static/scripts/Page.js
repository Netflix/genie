import T from "prop-types";
import React from "react";
import cookie from "react-cookie";
import $ from "jquery";

import SearchForm from "./components/SearchForm";
import NoSearchResult from "./components/NoSearchResult";
import Loading from "./components/Loading";
import SearchBar from "./components/SearchBar";
import Pagination from "./components/Pagination";

import Table from "./components/Table";
import TableHeader from "./components/TableHeader";
import TableBody from "./components/TableBody";
import Error from "./components/Error";
import ToggleTimeZone from "./components/ToggleTimeZone";

import { fetch, hasChanged } from "./utils";

export default class Page extends React.Component {
  static contextTypes = { router: T.object.isRequired };
  static propTypes = { location: T.object };

  constructor(props) {
    super(props);
    //load timezone preference
    const utcChecked = cookie.load("genie.utcChecked");
    this.state = {
      data: [],
      links: {},
      page: {},
      error: null,
      noSearchResult: false,
      showSearchForm: true,
      utcChecked: utcChecked == null ? true : JSON.parse(utcChecked) //default preference is UTC
    };
  }

  componentDidMount() {
    const { query } = this.props.location;
    this.loadPageData(query);
  }

  componentWillReceiveProps(nextProps) {
    const { query } = nextProps.location;
    if (hasChanged(query, this.props.location.query)) {
      this.loadPageData(query);
    }
  }

  get timeZone() {
    return this.state.utcChecked ? "UTC" : "PST";
  }

  toggleRowDetails = id =>
    this.setState({ rowId: this.state.rowId === id ? null : id });

  toggleSearchForm = () =>
    this.setState({ showSearchForm: !this.state.showSearchForm });

  toggleTimeZone = utcChecked => {
    // save tz preference in a cookie for future loads
    cookie.save("genie.utcChecked", utcChecked);
    this.setState({ utcChecked });
  };

  loadPageData(query) {
    if ($.isEmptyObject(query)) {
      query = { size: 100 };
    }
    const { rowId = null, showSearchForm = "true" } = query;
    // Empty data array indicates page is loading!!
    this.setState({ query, data: [] });

    fetch(this.url, query)
      .done(data => {
        if (data.hasOwnProperty("_embedded")) {
          this.setState({
            query,
            rowId,
            error: null,
            noSearchResult: false,
            page: data.page,
            links: data._links,
            data: data._embedded[this.dataKey],
            showSearchForm: $.parseJSON(showSearchForm)
          });
        } else {
          this.setState({ query, data: [], error: null, noSearchResult: true });
        }
      })
      .fail(xhr => {
        this.setState({ query, data: [], error: xhr.responseJSON });
      });
  }

  render() {
    const sideBar = this.state.showSearchForm ? (
      <div>
        <div className="text-muted search-hint">
          <i>** Use % for wildcard search</i>
        </div>
        <SearchForm
          query={this.state.query}
          formFields={this.formFields}
          hiddenFormFields={this.hiddenFormFields}
          toggleSearchForm={this.toggleSearchForm}
          searchPath={this.searchPath}
        />
      </div>
    ) : (
      <SearchBar toggleSearchForm={this.toggleSearchForm} />
    );

    let resultPanel = null;
    if (this.state.error != null) {
      resultPanel = <Error error={this.state.error} />;
    } else {
      resultPanel =
        this.state.data.length > 0 ? (
          <div
            className={this.state.showSearchForm ? "col-md-10" : "col-md-12"}
          >
            <Table>
              <TableHeader headers={this.tableHeader} />
              <TableBody
                rows={this.state.data}
                rowId={this.state.rowId}
                rowType={this.rowType}
                detailsTable={this.detailsTable}
                toggleRowDetails={this.toggleRowDetails}
                timeZone={this.timeZone}
              />
            </Table>
            <Pagination
              page={this.state.page}
              pageType={this.searchPath}
              links={this.state.links}
            />
          </div>
        ) : this.state.noSearchResult ? (
          <NoSearchResult />
        ) : (
          <Loading />
        ); // Default to loading...
    }

    return (
      <div className="row">
        {sideBar}
        <div className="timezone-toggle">
          <ToggleTimeZone
            checked={this.state.utcChecked}
            handleChange={this.toggleTimeZone}
          />
        </div>
        {resultPanel}
      </div>
    );
  }
}
