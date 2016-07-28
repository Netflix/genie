import React, { PropTypes as T } from 'react';
import { Link } from 'react-router';

import TableHeader from './components/TableHeader';
import SiteHeader from './components/SiteHeader';
import SiteFooter from './components/SiteFooter';
import filesize from 'filesize';

import { genieJobsUrl, fileUrl, momentFormat, fetch } from './utils';

export default class OutputDirectory extends React.Component {

  static propTypes = {
    headers : T.array,
    params  : T.object,
  }

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
      const [jobId, ...ignored] = url.split('/');
      this.setState({
        output, jobId, url,
      });
    });
  }

  render() {
    const infos = [{
      className: '',
      name: `Job Id: ${this.state.jobId ? this.state.jobId : 'NA'}`,
    }];

    return (
      <div>
        <SiteHeader headers={this.props.headers} infos={infos} />
        <div className="container job-output-directory">
          {
            this.state.output.files.length === 0 &&
              this.state.output.directories.length === 0 ?
              <div>No Output found </div> :
              <div>
                <HomeButton jobId={this.state.jobId} />
                <Table
                  headers={['Name', 'Size', 'Last Modified (UTC)']}
                  output={this.state.output}
                />
                <DirectoryInfo output={this.state.output} />
                <Navigation
                  url={this.state.url}
                  parent={this.state.output.parent}
                />
              </div>
          }
        </div>
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
    <td>{filesize(props.file.size)}</td>
    <td className="col-xs-3">{momentFormat(props.file.lastModified)}</td>
  </tr>;

FileRow.propTypes = {
  file: T.shape({
    name         : T.string,
    size         : T.number,
    url          : T.string,
    lastModified : T.string,
  }),
};

const DirectoryRow = (props) =>
  <tr>
    <td>
      <i className="fa fa-folder-o" aria-hidden="true"></i>
      <span className="output-listing">
        <Link to={genieJobsUrl(props.directory.url)}>{props.directory.name}</Link>
      </span>
    </td>
    <td>--</td>
    <td className="col-xs-3">{momentFormat(props.directory.lastModified)}</td>
  </tr>;

DirectoryRow.propTypes = {
  directory: T.shape({
    name         : T.string,
    size         : T.number,
    url          : T.string,
    lastModified : T.string,
  }),
};

const Table = (props) =>
  <table className="table">
    <TableHeader headers={props.headers} />
    <TableBody output={props.output} />
  </table>;

Table.propTypes = {
  headers: T.array,
  output : T.shape({
    files       : T.array,
    directories : T.array,
  }),
};

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

TableBody.propTypes = {
  output: T.shape({
    files       : T.array,
    directories : T.array,
  }),
};

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
  return <div />;
};

Navigation.propTypes = {
  url    : T.string,
  parent : T.shape({
    url  : T.string,
  }),
};

const DirectoryInfo = (props) =>
  <div className="pull-right directory-info">
    {`${props.output.files.length} File(s), ${props.output.directories.length} Folder(s)`}
  </div>;

DirectoryInfo.propTypes = {
  output: T.shape({
    files       : T.array,
    directories : T.array,
  }),
};

const HomeButton = (props) =>
  <div className="row">
    <span className="col-xs-3 fa-home-div">
      <Link to={`/output/${props.jobId}/output`}>
        <i className="fa fa-home fa-2x" aria-hidden="true"></i>
      </Link>
    </span>
  </div>;

HomeButton.propTypes = {
  jobId: T.string,
};
