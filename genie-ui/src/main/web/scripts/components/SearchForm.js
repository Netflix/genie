import React from "react";

import Select from "react-select";

import RangeCalendar from "rc-calendar/lib/RangeCalendar";
import Picker from "rc-calendar/lib/Picker";
import enUS from "rc-calendar/lib/locale/en_US";
import Panel from "rc-time-picker/lib/Panel";

import { momentFormat, milliSeconds, nowUtc } from "../utils";

import T from "prop-types";

import "rc-calendar/assets/index.css";
import "rc-time-picker/assets/index.css";

export default class SearchForm extends React.Component {
  static contextTypes = { router: T.object.isRequired };

  static propTypes = {
    query: T.object,
    formFields: T.arrayOf(T.object).isRequired,
    hiddenFormFields: T.arrayOf(T.object),
    toggleSearchForm: T.func.isRequired,
    searchPath: T.string.isRequired
  };

  static defaultProps = { hiddenFormFields: [] };

  constructor(props, context) {
    super(props, context);
    this.state = this.getDefaultFormState(props);
  }

  componentWillReceiveProps(nextProps) {
    const { query } = nextProps;
    let updateState = false;
    // check if query <-> form state are in sync
    // if not then update form state
    for (const key of Object.keys(query)) {
      if (!this.props.query || query[key] !== this.props.query[key]) {
        updateState = true;
        break;
      }
    }
    if (updateState) {
      const { formFields } = this.getDefaultFormState(nextProps);
      for (const name of Object.keys(formFields)) {
        // update form fields from query object
        if (query[name]) {
          if (name === "sort") {
            formFields.sortOrder.value = query[name].split(",").pop();
          }
          formFields[name].value = query[name];
        } else if (
          formFields[name].queryMapping &&
          this.includes(formFields[name].queryMapping, query)
        ) {
          const { mapper, queryMapping } = formFields[name];
          const queryValues = queryMapping.map(x => query[x]);
          formFields[name].value = queryValues.map(mapper);
        }
      }
      this.setState({ formFields });
    }
  }

  getDefaultFormState(props) {
    const formFields = {};
    for (const field of [...props.formFields, ...props.hiddenFormFields]) {
      formFields[field.name] = Object.assign({}, field); // new copy
    }
    return {
      formFields,
      linkText: "more",
      showAllFormFields: false,
      hasHiddenFormFields: props.hiddenFormFields.length > 0
    };
  }

  includes = (array, object) =>
    array.every(x => Object.keys(object).includes(x));

  isValidRange = range => range && range[0] && range[1];

  hasValue = field => field && (field.value > 0 || field.value.length > 0);

  // Mutators
  toggleFormFields = e => {
    e.preventDefault();
    this.setState({
      linkText: this.state.linkText === "more" ? "less" : "more",
      showAllFormFields: !this.state.showAllFormFields
    });
  };

  resetFormState = e => {
    e.preventDefault();
    this.setState(this.getDefaultFormState(this.props));
  };

  handleFormFieldChange = name => e => {
    const { formFields } = this.state;
    formFields[`${name}`].value = e.target.value.trim();
    this.setState({ formFields });
  };

  handleKeyPressChange = name => val => {
    const { formFields } = this.state;
    formFields[`${name}`].value = val;
    this.setState({ formFields });
  };

  handleSortOrderChange = e => {
    e.preventDefault();
    const { formFields } = this.state;
    formFields.sortOrder.value = e.target.value;
    this.setState({ formFields });
  };

  handleSearch = e => {
    e.preventDefault();
    const query = { src: "btn" };
    for (const [name, field] of Object.entries(this.state.formFields)) {
      if (this.hasValue(field)) {
        if (name === "sortOrder") {
          const { sort } = this.state.formFields;
          if (!this.hasValue(sort)) continue;
          // if sort value is not set discard sortOrder
          const values = this.state.formFields.sort.value.split(",");
          if (values.includes("asc") || values.includes("desc")) values.pop();
          // remove old
          values.push(field.value);
          // add new
          query.sort = values.join();
        } else if (field.type === "timeRange") {
          // use queryMapping values for keys
          // queryMapping and value ordering is same
          const { queryMapping, value } = field;
          queryMapping.forEach((key, index) => {
            query[`${key}`] = milliSeconds(value[index]);
          });
        } else {
          query[name] = field.value;
        }
      }
    }
    this.context.router.push({ query, pathname: this.props.searchPath });
  };

  render() {
    return (
      <div className="col-xs-2 form-job-search">
        <div className="pull-right form-group">
          <a
            href="javascript:void(0)"
            onClick={() => this.props.toggleSearchForm()}
          >
            <i className="fa fa-navicon fa-lg" aria-hidden="true" />
          </a>
        </div>
        <form className="form-horizontal" role="search">
          {this.props.formFields.map((field, index) => {
            switch (field.type) {
              case "input":
                return (
                  <div key={`${index}-div`} className="form-group">
                    <label key={`${index}-label`} className="form-label">
                      {field.label}:
                    </label>
                    <input
                      key={index}
                      type="text"
                      value={this.state.formFields[field.name].value}
                      onChange={this.handleFormFieldChange(field.name)}
                      className="form-control"
                    />
                  </div>
                );
              case "select":
                return (
                  <div key={`${index}-div`} className="form-group">
                    <label key={`${index}-label`} className="form-label">
                      {field.label}:
                    </label>
                    <Select
                      multi
                      simpleValue
                      value={this.state.formFields[field.name].value}
                      options={field.selectFields}
                      onChange={this.handleKeyPressChange(field.name)}
                    />
                  </div>
                );
              case "option":
                return (
                  <div key={`${index}-div`} className="form-group">
                    <label key={`${index}-label`} className="form-label">
                      {field.label}:
                    </label>
                    <select
                      className="form-control"
                      value={this.state.formFields[field.name].value}
                      onChange={this.handleFormFieldChange(field.name)}
                    >
                      {field.optionValues.map((value, idx) =>
                        <option key={idx} value={value}>
                          {value}
                        </option>
                      )}
                    </select>
                  </div>
                );
              case "timeRange":
                const calendar = (
                  <RangeCalendar
                    showWeekNumber={false}
                    dateInputPlaceholder={["min", "max"]}
                    defaultValue={[nowUtc(), nowUtc()]}
                    locale={enUS}
                    timePicker={<Panel />}
                  />
                );
                return (
                  <div key={`${index}-div`} className="form-group">
                    <label key={`${index}-label`} className="form-label">
                      {field.label}:
                    </label>
                    <Picker
                      value={this.state.formFields[field.name].value}
                      onChange={this.handleKeyPressChange(field.name)}
                      animation="slide-up"
                      calendar={calendar}
                    >
                      {({ value }) => {
                        return (
                          <input
                            className="form-control ant-calendar-picker-input ant-input"
                            value={
                              (this.isValidRange(value) &&
                                `${momentFormat(value[0])} - ${momentFormat(
                                  value[1]
                                )}`) ||
                              ""
                            }
                          />
                        );
                      }}
                    </Picker>
                  </div>
                );
              case "sortOption":
                return (
                  <div
                    key={`${index}-sortOption`}
                    className={
                      this.state.formFields.sort.value === "" ? "hidden" : ""
                    }
                  >
                    <div key={`${index}-div`} className="form-group">
                      <label key={`${index}-label`} className="form-label">
                        {field.label}:
                      </label>
                      <select
                        className="form-control"
                        value={this.state.formFields.sortOrder.value}
                        onChange={this.handleSortOrderChange}
                      >
                        {field.optionValues.map((value, idx) =>
                          <option key={`${idx}-option`} value={value}>
                            {value}
                          </option>
                        )}
                      </select>
                    </div>
                  </div>
                );
              default:
                return null;
            }
          })}
          <div className={this.state.showAllFormFields ? "" : "hidden"}>
            {this.props.hiddenFormFields.map((field, index) =>
              <div key={`${index}-div`} className="form-group">
                <label key={`${index}-label`} className="form-label">
                  {field.label}:
                </label>
                <input
                  key={index}
                  type="text"
                  value={this.state.formFields[field.name].value}
                  onChange={this.handleFormFieldChange(field.name)}
                  className="form-control"
                />
              </div>
            )}
          </div>
          {this.state.hasHiddenFormFields
            ? <div className="form-group">
                <a href="javascript:void(0)" onClick={this.toggleFormFields}>
                  Show {this.state.linkText}
                </a>
              </div>
            : null}
          <div className="form-group">
            <button
              type="submit"
              className="btn btn-primary"
              onClick={this.handleSearch}
            >
              Search
            </button>
            <a
              href="javascript:void(0)"
              onClick={this.resetFormState}
              className="reset-link"
            >
              reset
            </a>
          </div>
        </form>
      </div>
    );
  }
}
