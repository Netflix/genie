import React, { PropTypes } from 'react';
import { momentFormat } from '../utils';

const TableRow = (props) =>
  <tr>
    <td>
      <a
        target="_blank"
        href="javascript:void(0)"
        value={props.row.id}
        onClick={() => props.setShowDetails(props.row.id)}
      >
        {props.row.id}
      </a>
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
  row: PropTypes.shape({
    id      : PropTypes.string,
    name    : PropTypes.string,
    user    : PropTypes.string,
    status  : PropTypes.string,
    version : PropTypes.string,
    tags    : PropTypes.arrayOf(PropTypes.string),
    created : PropTypes.string,
    updated : PropTypes.string,
  }),
};

export default TableRow;
