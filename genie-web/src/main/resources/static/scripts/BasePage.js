import React from 'react';
import $ from 'jquery';

import { hasChanged } from './utils';

export default class BasePage extends React.Component {
  static propTypes = {
    location: React.PropTypes.object,
  }

  static childContextTypes = {
    location: React.PropTypes.object.isRequired,
  }

  getChildContext() {
    return { location: this.props.location };
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

  loadPageData(query) {
    if ($.isEmptyObject(query)) {
      query = { size: 25 };
    }

    this.loadData(query); // Implemented by Subclass
  }

  toggleSearchForm = () => {
    this.setState({ showSearchForm: !this.state.showSearchForm });
  }
}
