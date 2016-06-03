import React, { PropTypes } from 'react';

import Select from 'react-select';

export default class JobSearchForm extends React.Component {
  static propTypes = {
    formFields       : PropTypes.arrayOf(PropTypes.object),
    query            : PropTypes.object,
    searchPath       : PropTypes.string,
    toggleSearchForm : PropTypes.func,
  }

  static contextTypes = {
    router: React.PropTypes.object.isRequired,
  }

  constructor(props, context) {
    super(props, context);
    this.sortByFields = ['user', 'created', 'id', 'name', 'status', 'clusterName', 'cluserId'].map((field) => {
      return {
        value: field,
        label: field,
      };
    });

    this.state = this.getDefaultState();
  }

  componentWillReceiveProps(nextProps) {
    const { query } = nextProps;
    let updateState = false;
    for (const key of Object.keys(query)) {
      if (!this.props.query || query[key] !== this.props.query[key]) {
        updateState = true;
        break;
      }
    }
    if (updateState) {
      const formFields = {};
      for(const field of Object.keys(this.getDefaultState().formFields)){
        if(query[field]) {
          formFields[field] = query[field]
        } else {
          formFields[field] = this.getDefaultState().formFields[field]
        }
      }
      this.setState({ formFields });
    }
  }

  getDefaultState() {
    const formFields = {
      user        : '',
      status      : '',
      id          : '',
      name        : '',
      clusterName : '',
      cluserId    : '',
      commandName : '',
      commandId   : '',
      tag         : '',
      sort        : '',
      size        : 25,
    };

    return {
      formFields,
      showAllFormFields : false,
      sortOrder         : 'desc',
      linkText          : 'more',
      sortByFields      : this.sortByFields,
    };
  }

  handleSelectChange = (val) => {
    const { formFields } = this.state;
    formFields.sort = val;
    this.setState({ formFields });
  }

  handleSortOrderChange = (e) => {
    e.preventDefault();
    const { formFields } = this.state;
    const sortOrder = e.target.value;
    this.setState({ formFields, sortOrder });
  }

  toggleFormFields = (e) => {
    e.preventDefault();
    this.setState({
      linkText          : this.state.linkText === 'more' ? 'less' : 'more',
      showAllFormFields : !this.state.showAllFormFields,
    });
  }

  handleFormFieldChange = (key) => {
    return (e) => {
      const { formFields } = this.state;
      formFields[`${key}`] = e.target.value.trim();
      this.setState({ formFields });
    };
  }

  handleSearch = (e) => {
    e.preventDefault();
    const query = {};
    for (const field of Object.keys(this.state.formFields)) {
      if (this.state.formFields[field] !== '') {
        if (field === 'sort') {
          const values = this.state.formFields[field].split(',');
          let spliceIndex = 0;
          let start = values.length;
          if (values.includes('asc') || values.includes('desc')) {
            spliceIndex = 1;
            start = values.length - 1;
          }
          values.splice(start, spliceIndex, this.state.sortOrder);
          query[field] = values.join();
        } else {
          query[field] = this.state.formFields[field];
        }
      }
    }

    this.context.router.push({
      query,
      pathname : 'jobs',
    });
  }

  resetFormFields = (e) => {
    e.preventDefault();
    this.setState(this.getDefaultState());
  }

  render() {
    return (
      <div>
        <div className="col-xs-2 form-job-search">
          <div className="pull-right form-group">
            <a href="javascript:void(0)" onClick={() => this.props.toggleSearchForm()}>
              <i className="fa fa-navicon fa-lg" aria-hidden="true"></i>
            </a>
          </div>
          <div>
            <form className="form-horizontal" role="search">
              <div className="form-group">
                <label className="form-label">User Name:</label>
                <input
                  type="text"
                  value={this.state.formFields.user}
                  onChange={this.handleFormFieldChange('user')}
                  className="form-control"
                />
              </div>
              <div className="form-group">
                <label className="form-label">Job ID:</label>
                <input
                  type="text"
                  value={this.state.formFields.id}
                  onChange={this.handleFormFieldChange('id')}
                  className="form-control"
                />
              </div>
              <div className="form-group">
                <label className="form-label">Job Name:</label>
                <input
                  type="text"
                  value={this.state.formFields.name}
                  onChange={this.handleFormFieldChange('name')}
                  className="form-control"
                />
              </div>
              <div className="form-group">
                <label className="form-label">Status:</label>
                <select
                  className="form-control"
                  value={this.state.formFields.status}
                  onChange={this.handleFormFieldChange('status')}
                >
                  <option></option>
                  <option>RUNNING</option>
                  <option>SUCCEEDED</option>
                  <option>FAILED</option>
                  <option>KILLED</option>
                </select>
              </div>
              <div className="form-group">
                <label className="form-label">Size:</label>
                <select
                  className="form-control"
                  value={this.state.formFields.size}
                  onChange={this.handleFormFieldChange('size')}
                >
                  <option value="10">10</option>
                  <option value="25">25</option>
                  <option value="50">50</option>
                  <option value="100">100</option>
                </select>
              </div>
              <div className="form-group">
                <label className="form-label">Sort by</label>
                <Select
                  multi
                  simpleValue
                  value={this.state.formFields.sort}
                  options={this.state.sortByFields}
                  onChange={this.handleSelectChange}
                />
              </div>
              <div className={this.state.formFields.sort === '' ? 'hidden' : ''}>
                <div className="form-group">
                  <label className="form-label">Order:</label>
                  <select
                    className="form-control"
                    value={this.state.sortOrder}
                    onChange={this.handleSortOrderChange}
                  >
                    <option value="asc">asc</option>
                    <option value="desc">desc</option>
                  </select>
                </div>
              </div>
              <div className={this.state.showAllFormFields ? '' : 'hidden'}>
                <div className="form-group">
                  <label className="form-label">Tags (csv):</label>
                  <input
                    type="text"
                    value={this.state.formFields.tag}
                    onChange={this.handleFormFieldChange('tag')}
                    className="form-control"
                  />
                </div>
                <div className="form-group">
                  <label className="form-label">Cluster Name:</label>
                  <input
                    type="text"
                    value={this.state.formFields.clusterName}
                    onChange={this.handleFormFieldChange('clusterName')}
                    className="form-control"
                  />
                </div>
                <div className="form-group">
                  <label className="form-label">Cluster ID:</label>
                  <input
                    type="text"
                    value={this.state.formFields.cluserId}
                    onChange={this.handleFormFieldChange('cluserId')}
                    className="form-control"
                  />
                </div>
                <div className="form-group">
                  <label className="form-label">Command Name:</label>
                  <input
                    type="text"
                    value={this.state.formFields.commandName}
                    onChange={this.handleFormFieldChange('commandName')}
                    className="form-control"
                  />
                </div>
                <div className="form-group">
                  <label className="form-label">Command ID:</label>
                  <input
                    type="text"
                    value={this.state.formFields.commandId}
                    onChange={this.handleFormFieldChange('commandId')}
                    className="form-control"
                  />
                </div>
              </div>
              <div className="form-group">
                <a href="javascript:void(0)" onClick={this.toggleFormFields}>Show {this.state.linkText}</a>
              </div>
              <div className="form-group">
                <button type="submit" className="btn btn-primary" onClick={this.handleSearch}>Search</button>
                <a href="javascript:void(0)" onClick={this.resetFormFields} className="reset-link">reset</a>
              </div>
            </form>
          </div>
        </div>
      </div>
    );
  }
}
