import React from 'react';
import { render } from 'react-dom';
import { Link } from 'react-router';
import { fetch } from './utils';
import moment from 'moment';
import TableHeader from './components/TableHeader';
import SiteHeader from './components/SiteHeader';
import SiteFooter from './components/SiteFooter';
import { genieJobsUrl, fileUrl, momentFormat } from './utils';

export default class GenieJobDirectory extends React.Component {
  static defaultProps = {
    headers: [
      { url       : '#',
        name      : 'GENIE',
        className : 'supress',
      },
    ],
  }

  constructor(props) {
    super(props);
    this.state = {
      output: {
        files       : [],
        directories : [],
      },
      url: '',
    };
  }

  componentDidMount() {
    const { splat } = this.props.params;
    this.loadData(splat);
  }

  componentWillReceiveProps(nextProps) {
    const { splat } = nextProps.params;
    this.loadData(splat);
  }

  loadData(url) {
    fetch(`/api/v3/jobs/${url}`)
    .done((output) => {
      const [jobId, ...others] = url.split('/');
      this.setState({
        output, jobId, url
      });
    });
  }

  render() {
    return (
      <div>
        <SiteHeader headers={this.props.headers} />
        <div className="container job-output-directory">
          <HomeButton jobId={this.state.jobId} />
          <Table
            headers={['Name', 'Size', 'Last Modified']}
            output={this.state.output}
          />
          <DirectoryInfo output={this.state.output} />
          <Navigation
            url={this.state.url}
            parent={this.state.output.parent}
          />
        </div>
        <div className='pull-right job-id-directory-info'>Job Id: {this.state.jobId}</div>
        <SiteFooter />
      </div>
    );
  }
}

const FileRow = (props) =>
  <tr>
    <td>
      <i className="fa fa-file-o" aria-hidden="true"></i>
      <span className="output-listing">
        <Link target="_blank" to={fileUrl(props.file.url)}>{props.file.name}</Link>
      </span>
    </td>
    <td>{props.file.size} kb</td>
    <td className="col-xs-3">{momentFormat(props.file.lastModified)}</td>
  </tr>;

const DirectoryRow = (props) =>
  <tr>
    <td>
      <i className="fa fa-folder-o" aria-hidden="true"></i>
      <span className="output-listing">
        <Link to={genieJobsUrl(props.directory.url)}>{props.directory.name}</Link>
      </span>
    </td>
    <td>{props.directory.size} kb</td>
    <td className="col-xs-3">{momentFormat(props.directory.lastModified)}</td>
  </tr>;


const Table = (props) =>
  <table className="table">
    <TableHeader headers={props.headers} />
    <TableBody output={props.output} />
  </table>;

const TableBody = (props) =>
  <tbody>
    {props.output.directories.map((directory, index) =>
      <DirectoryRow
        key={index}
        directory={directory}
      />
    )}
    {props.output.files.map((file, index) =>
      <FileRow
        key={index}
        file={file}
      />
    )}
  </tbody>;

const Navigation = (props) => {
  if (props.url.split('/').length > 2) {
    return (
      <div>
        <Link to={genieJobsUrl(props.parent.url)}>
          <i className="fa fa-arrow-left" aria-hidden="true"></i> back
        </Link>
      </div>
    );
  }
  return <div/>;
};

const DirectoryInfo = (props) =>
  <div className="pull-right directory-info">
    {`${props.output.files.length} File(s), ${props.output.directories.length} Folder(s)`}
  </div>;

const HomeButton = (props) =>
  <div className="row">
    <span className="col-xs-3 fa-home-div">
      <Link to={`/output/${props.jobId}/output`}>
        <i className="fa fa-home fa-2x" aria-hidden="true"></i>
      </Link>
    </span>
  </div>;
