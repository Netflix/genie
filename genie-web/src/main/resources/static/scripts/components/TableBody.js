import React, { PropTypes } from 'react';

export default class TableBody extends React.Component {
  static propTypes = {
    rows : PropTypes.arrayOf(PropTypes.object),
  }

  static contextTypes = {
    location : PropTypes.object.isRequired,
    router   : PropTypes.object.isRequired,
  }

  constructor(props) {
    super(props);
    this.state = this.defaultState;
  }

  componentDidMount() {
    const { query } = this.context.location || {};
    if (query.showDetails) {
      this.showDetails(query.showDetails);
    } else {
      this.setState(this.defaultState);
    }
  }

  componentWillReceiveProps(nextProps, nextContext) {
    const { query } = nextContext.location || {};
    if (query.showDetails) {
      this.showDetails(query.showDetails);
    } else {
      this.setState(this.defaultState);
    }
  }

  setShowDetails = (id) => {
    const { query, pathname } = this.context.location;
    query.showDetails = `${id}`; // TODO: Use immutable DS
    this.context.router.push({
      query,
      pathname,
    });
  }

  get defaultState() {
    return {
      row         : {},
      index       : -1,
      showDetails : false,
    };
  }

  hideDetails = () => {
    const { query, pathname } = this.context.location;
    delete query.showDetails; // TODO: Use immutable DS
    this.context.router.push({
      query,
      pathname,
    });
  }

  showDetails = (id) => {
    const row = this.props.rows.find((row) => row.id === id);
    const index = this.props.rows.findIndex((row) => row.id === id);
    this.setState({
      row,
      index,
      showDetails : true,
    });
  }

  construct = (TableRow) => {
    const tableRows = this.props.rows.map((row, index) => {
      return (
        <TableRow
          key={index}
          row={row}
          setShowDetails={this.setShowDetails}
        />
      );
    });

    return tableRows;
  }
}
