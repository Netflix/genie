import React, { PropTypes as T } from 'react';
import { momentFormat } from '../utils';

import { Link } from 'react-router';

const TableRow = (props) =>
  <tr>
    <td>
      <Link
        target="_blank"
        to={`${props.searchPath}?name=${props.row.name}&showDetails=${props.row.id}`}
      >
        {props.row.id}
      </Link>
    </td>
    <td>{props.row.name}</td>
    <td>{props.row.user}</td>
    <td>{props.row.status}</td>
    <td>{props.row.version}</td>
    <td>
      <ul>
        {props.row.tags.map((tag, index) =>
          <li key={index}>{tag}</li>
        )}
      </ul>
    </td>
    <td className="col-xs-2">{momentFormat(props.row.created)}</td>
    <td className="col-xs-2">{momentFormat(props.row.updated)}</td>
  </tr>;

TableRow.propTypes = {
  searchPath: T.string.isRequired,
  row: T.shape({
    id      : T.string,
    name    : T.string,
    user    : T.string,
    status  : T.string,
    version : T.string,
    tags    : T.arrayOf(T.string),
    created : T.string,
    updated : T.string,
  }),
};

export default TableRow;
