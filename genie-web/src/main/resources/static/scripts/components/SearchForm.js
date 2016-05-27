import React from 'react';
import { render } from 'react-dom';
import Select from 'react-select';

export default class SearchForm extends React.Component {
  static contextTypes = {
    router: React.PropTypes.object.isRequired
  }

  static propTypes = {
    formFields       : React.PropTypes.array,
    query            : React.PropTypes.object,
    searchPath       : React.PropTypes.string,
    toggleSearchForm : React.PropTypes.func,
  }

  constructor(props, context) {
    super(props, context);
    this.state = this.getFormState(props);
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
      const formFields = Object.assign(this.getFormState(nextProps).formFields, query);
      this.setState({ formFields });
    }
  }

  getFormState(props) {
    const formFields = {}
    for(let field of props.formFields) {
      formFields[field.name] = field.value;
    }

    return {
      formFields   : formFields,
      sortOrder    : 'desc',
    };
  }

  handleFormFieldChange = (key) => {
    return (e) => {
      let { formFields } = this.state;
      formFields[`${key}`] = e.target.value.trim();
      this.setState({ formFields });
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

  handleSearch = (e) => {
    e.preventDefault();
    let query = {};
    for (const field of Object.keys(this.state.formFields)) {
      if (this.state.formFields[field] !== '') {
        if(field === 'sort') {
          let values = this.state.formFields[field].split(",");
          let spliceIndex = 0;
          let start = values.length
          if (values.includes('asc') || values.includes('desc')) {
            spliceIndex = 1;
            start = values.length - 1;
          }
          values.splice(start, spliceIndex, this.state.sortOrder)
          query[field] = values.join();
        } else {
          query[field] = this.state.formFields[field];
        }
      }
    }

    this.context.router.push({
      query    : query,
      pathname : this.props.searchPath,
    });
  }

  resetFormFields = (e) => {
    e.preventDefault();
    this.setState({formFields: this.getFormState(this.props).formFields});
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
          <form className="form-horizontal" role="search">
            {this.props.formFields.map((field, index) => {
              switch (field.type) {
                case 'input':
                  return (
                   <div key={`${index}-div`} className="form-group">
                    <label key={`${index}-label`} className="form-label">{field.label}:</label>
                    <input
                      key={index}
                      type="text"
                      value={this.state.formFields[field.name]}
                      onChange={this.handleFormFieldChange(field.name)}
                      className="form-control"
                    />
                  </div>
                );
                case 'select':
                  return (
                    <div key={`${index}-div`} className="form-group">
                      <label key={`${index}-label`} className="form-label">Sort by</label>
                      <Select
                        multi
                        simpleValue
                        value={this.state.formFields[field.name]}
                        options={field.selectFields}
                        onChange={this.handleSelectChange}
                      />
                    </div>
                  );
              }
            }
            )}
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
            <div className="form-group">
              <button type="submit" className="btn btn-primary" onClick={this.handleSearch}>Search</button>
              <a href="javascript:void(0)" onClick={this.resetFormFields} className="reset-link">reset</a>
            </div>
          </form>
        </div>
      </div>
    );
  }
}
