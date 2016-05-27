import React from 'react';
import { render } from 'react-dom';

export default class TableBody extends React.Component {
  static contextTypes = {
    location : React.PropTypes.object.isRequired,
    router   : React.PropTypes.object.isRequired,
  }

  constructor(props){
    super(props);
    this.state = this.defaultState();
  }

  componentDidMount() {
    const { query } = this.context.location || {};
    if (query.showDetails) {
      this.showDetails(query.showDetails);
    } else {
      this.setState(this.defaultState());
    }
  }

  componentWillReceiveProps(nextProps, nextContext) {
    const { query } = nextContext.location || {};
    if (query.showDetails) {
      this.showDetails(query.showDetails);
    } else {
      this.setState(this.defaultState());
    }
  }

  defaultState() {
    return {
      row         : {},
      index       : -1,
      showDetails : false,
    }
  }

  setShowDetails = (id) => {
    const { query, pathname } = this.context.location;
    query.showDetails = `${id}`; //TODO: Use immutable DS
    this.context.router.push({
      query,
      pathname,
    });
  }

  hideDetails = () => {
    const { query, pathname } = this.context.location;
    delete query.showDetails; //TODO: Use immutable DS
    this.context.router.push({
      query,
      pathname,
    });
  }

  showDetails = (id) => {
    const row = this.props.rows.find((row) => row.id === id);
    const index = this.props.rows.findIndex((row) => row.id === id);
    this.setState({
      showDetails : true,
      row         : row,
      index       : index,
    });
  }

  construct = (TableRow) => {
    let tableRows = this.props.rows.map((row, index) => {
      return (
        <TableRow
          key={row.id}
          row={row}
          setShowDetails={this.setShowDetails}
        />
      );
      });

    return tableRows;
  }
}
