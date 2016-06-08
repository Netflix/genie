import React, { PropTypes } from 'react';
import $ from 'jquery';

import SearchForm from './components/SearchForm';
import NoSearchResult from './components/NoSearchResult';
import Loading from './components/Loading';
import SearchBar from './components/SearchBar';
import Pagination from './components/Pagination';
import Table from './components/Table';

import { fetch, hasChanged } from './utils';

export default class Page extends React.Component {
  static contextTypes = {
    router : PropTypes.object.isRequired,
  }

  static propTypes = {
    location : PropTypes.object,
  }

  constructor(props) {
    super(props);
    this.state = {
      data           : [],
      links          : {},
      page           : {},
      showSearchForm : true,
      noSearchResult : false,
    };
  }

  componentDidMount() {
    const { query } = this.props.location;
    this.loadPageData(query);
  }

  componentWillReceiveProps(nextProps) {
    const { query } = nextProps.location;
    if (hasChanged(query,
                   this.props.location.query)) {
      this.loadPageData(query);
    }
  }

  setRowId = (id) => {
    const { query, pathname } = this.props.location;
    query.showDetails = `${id}`;
    query.src = 'link';
    this.context.router.push({
      query,
      pathname,
    });
  }

  loadPageData(query) {
    if ($.isEmptyObject(query)) {
      query = { size: 25 };
    }

    fetch(this.url, query)
    .done((data) => {
      if (data.hasOwnProperty('_embedded')) {
        this.setState({
          query,
          noSearchResult : false,
          page           : data.page,
          links          : data._links,
          data           : data._embedded[this.dataKey],
        });
      } else {
        this.setState({
          noSearchResult: true,
          query,
          data  : [],
        });
      }
    });
  }

  toggleSearchForm = () => {
    this.setState({ showSearchForm: !this.state.showSearchForm });
  }

  hideDetails = () => {
    const { query, pathname } = this.props.location;
    delete query.showDetails;
    delete query.src;
    this.context.router.push({
      query,
      pathname,
    });
  }

  get searchResultsTable() {
    return (
      <Table
        header={this.tableHeader}
        body={this.tableBody}
      />
    );
  }

  render() {
    const sideBar = this.state.showSearchForm ?
      <SearchForm
        query={this.state.query}
        formFields={this.formFields}
        hiddenFormFields={this.hiddenFormFields}
        toggleSearchForm={this.toggleSearchForm}
        searchPath={this.searchPath}
      /> :
      <SearchBar toggleSearchForm={this.toggleSearchForm} />;

    const searchResult = this.state.data.length > 0 ?
      <div className={this.state.showSearchForm ? 'col-md-10' : 'col-md-12'}>
        {this.searchResultsTable}
        <Pagination
          page={this.state.page}
          pageType={this.searchPath}
          links={this.state.links}
        />
      </div> :
      this.state.noSearchResult ?
        <NoSearchResult /> :
        <Loading />;

    return (
      <div className="row" >
        {sideBar}
        {searchResult}
      </div>
    );
  }
}
