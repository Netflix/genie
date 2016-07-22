import React, { PropTypes as T } from 'react';
import { fetch } from './utils';

import cookie from 'react-cookie';

import SiteHeader from './components/SiteHeader';
import SiteFooter from './components/SiteFooter';

export default class App extends React.Component {
  static propTypes = {
    headers  : T.array,
    children : T.element.isRequired,
  }

  static defaultProps = {
    headers: [
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
    ],
  }

  constructor(props) {
    super(props);
    this.state = {
      version: '',
      infos: [
        {
          className : '',
          name      : cookie.load('genie.user'),
        },
      ],
    };
  }

  componentDidMount() {
    this.loadData();
  }

  loadData() {
    fetch('/actuator/info', null, 'GET', 'application/json')
    .done((data) => {
      this.setState({ version: data.genie.version });
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
