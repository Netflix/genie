import React, { PropTypes as T } from 'react';
import { Link } from 'react-router';

import TableHeader from './components/TableHeader';
import SiteHeader from './components/SiteHeader';
import SiteFooter from './components/SiteFooter';
import filesize from 'filesize';

import { genieJobsUrl, fileUrl, momentFormat, fetch } from './utils';

export default class OutputDirectory extends React.Component {

  static propTypes = {
    params  : T.object,
  }

  constructor(props) {
    super(props);
    this.state = {
      output: {
        files       : [],
        directories : [],
      },
      url: '',
      headers: [],
      infos: [],
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
    .done(output => {
      const [jobId, ...ignored] = url.split('/');
      const genieSiteUrl = `/jobs?id=${jobId}&rowId=${jobId}`;

      this.setState({
        output,
        jobId,
        url,
        headers: [
          { url       : '#',
            name      : 'GENIE',
            className : 'supress',
          },
        ],
        infos: [{
          url       : genieSiteUrl,
          name      : <div><i className="fa fa-search" aria-hidden="true"></i> Job Id: {jobId}</div>,
          className : 'supress',
        }],
      });
    });
  }

  render() {
    return (
      <div>
        <SiteHeader
          headers={this.state.headers}
          infos={this.state.infos}
        />
        <div className="container job-output-directory">
          {
            this.state.output.files.length === 0 &&
              this.state.output.directories.length === 0 ?
              <div>No Output found </div> :
              <div>
                <Navigation url={this.state.url} />
                <Table>
                  <TableHeader headers={['Name', 'Size', 'Last Modified (UTC)']} />
                  <TableBody output={this.state.output} />
                </Table>
                <DirectoryInfo output={this.state.output} />
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
        <a href={fileUrl(props.file.url)}>{props.file.name}</a>
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
    {props.children}
  </table>;

Table.propTypes = {
  children : T.array,
  headers  : T.array,
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

const Navigation = (props) => {
  const [jobId, output, ...path] = props.url.split('/');
  let breadCrumbs = [];
  // Home Button
  breadCrumbs.push(
    <li key={jobId}>
      <Link to={`/output/${jobId}/output`}>
        <i className="fa fa-home fa-2x" aria-hidden="true"></i>
      </Link>
    </li>
  );
  // Directory links
  path.forEach((name, index) => {
    if (index === path.length - 1) {
      breadCrumbs.push(
        <li key={name} className="active">
          {name}
        </li>
      );
    } else {
      const fullPath = path.slice(0, index + 1).join('/');
      breadCrumbs.push(
        <li key={index}>
          <Link to={`/output/${jobId}/${output}/${fullPath}`}>{name}</Link>
        </li>
      );
    }
  });

  return (
    <ol className="breadcrumb">
      {breadCrumbs}
    </ol>
  );
};

Navigation.propTypes = {
  url : T.string,
};
