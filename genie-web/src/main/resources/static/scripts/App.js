import React from 'react';
import { render } from 'react-dom';
import $ from 'jquery';

import cookie from 'react-cookie';

import SiteHeader from './components/SiteHeader';
import SiteFooter from './components/SiteFooter';

export default class App extends React.Component {

  static defaultProps = {
    headers : [
      { url       : '/jobs',
        name      : 'GENIE',
        className : 'supress',
      },
      { url       : '/jobs',
        name      : 'Jobs',
        className : 'active',
      },
      { url       : '/clusters',
        name      : 'Clusters',
        className : 'active',
      },
      { url       : '/commands',
        name      : 'Commands',
        className : 'active',
      },
      { url       : '/applications',
        name      : 'Applications',
        className : 'active',
      },
    ]
  }

  constructor(props) {
    super(props);
    this.state = {
      version: '',
      infos: [
        {
          name      : cookie.load('genie.user'),
          className : ''
        }
      ]
    };
  }

  componentDidMount() {
    this.loadData();
  }

  loadData() {
    $.ajax({
      global: false,
      type: 'GET',
      headers: {
          'Accept': 'application/json'
      },
      url: '/actuator/info'
    }).done((data) => {
      this.setState({version: data.genie.version});
    });
  }

  render() {
    return (
      <div>
        <SiteHeader headers={this.props.headers} infos={this.state.infos} />
        <div className="container">
          {this.props.children}
        </div>
        <SiteFooter version={this.state.version} />
      </div>
    );
  }
}
