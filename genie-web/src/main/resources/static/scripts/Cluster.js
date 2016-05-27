import React from 'react';
import { render } from 'react-dom';
import $ from 'jquery';
import { fetch, hasChanged } from './utils';

import SearchResult from './components/SearchResult';
import NoSearchResult from './components/NoSearchResult';
import SearchBar from './components/SearchBar';
import SearchForm from './components/SearchForm';

import TableBody from './components/ClusterTableBody';
import TableHeader from './components/TableHeader';

export default class Cluster extends React.Component {
  static childContextTypes = {
    location: React.PropTypes.object.isRequired
  }

  getChildContext() {
    return { location: this.props.location }
  }

  constructor(props) {
    super(props);
    this.state = {
      clusters       : [],
      links          : {},
      page           : {},
      showSearchForm : true,
    };
  }

  componentDidMount() {
    const { query } = this.props.location;
    this.loadData(query);
  }

  componentWillReceiveProps(nextProps) {
    const { query } = nextProps.location;

    if (hasChanged(query,
                   this.props.location.query)) {
      this.loadData(query);
    }
  }

  loadData(query={size: 25}) {
    fetch('/api/v3/clusters', query)
    .done((data) => {
      if (data.hasOwnProperty('_embedded')) {
        this.setState({
          noSearchResult : false,
          query          : query,
          page           : data.page,
          links          : data._links,
          clusters       : data._embedded.clusterList,
        });
      }
      else {
        this.setState({
          query : query,
          clusters  : [],
        });
      }
    });
  }

  toggleSearchForm = () => {
    this.setState({showSearchForm: !this.state.showSearchForm});
  }

  render() {
    let search = this.state.showSearchForm ?
                  <SearchForm
                    query={this.state.query}
                    toggleSearchForm={this.toggleSearchForm}
                    formFields={[
                      {
                        label : 'Name',
                        name  : 'name',
                        value : '',
                        type  : 'input'
                      }, {
                        label : 'Status',
                        name  : "status",
                        value : '',
                        type  : 'input'
                      }, {
                        label : 'Tag',
                        name  : 'tag',
                        value : '',
                        type  : 'input'
                      }, {
                        label : 'Sort By',
                        name  : 'sort',
                        value : '',
                        type  : 'select',
                        selectFields: ["name", "status", "tag"].map((field) => {
                              return {
                                value: field,
                                label: field
                              }
                        }),
                      },
                    ]}
                    searchPath="clusters"
                  />:
                  <SearchBar toggleSearchForm={this.toggleSearchForm} />;

    let searchResult = this.state.clusters.length > 0 ?
                        <SearchResult
                          data={this.state.clusters}
                          links={this.state.links}
                          page={this.state.page}
                          pageType="clusters"
                          headers={['Id', 'Name', 'User', 'Status', 'Version', 'Tags', 'Created', 'Updated']}
                          showSearchForm={this.state.showSearchForm}
                          TableBody={TableBody}
                          TableHeader={TableHeader}
                        />:
                        <NoSearchResult />;


  return (
    <div className="row" >
      {search}
      {searchResult}
    </div>
  );
  }
}
