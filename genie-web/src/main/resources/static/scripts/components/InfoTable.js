import React, { PropTypes } from 'react';
import { Link } from 'react-router';

const InfoTable = (props) =>
  <table className="table">
    <thead>
      <tr>
        <td>Id</td>
        <td>Name</td>
      </tr>
    </thead>
    <tbody>
      {props.data.map((info, index) =>
        <tr key={index}>
          <td>
            <Link
              target="_blank"
              to={`${props.type}?name=${info.name}&showDetails=${info.id}`}
            >
              {info.id}
            </Link>
          </td>
          <td>{info.name}</td>
        </tr>
      )}
    </tbody>
  </table>;

InfoTable.propTypes = {
  type : PropTypes.string,
  data: PropTypes.arrayOf(PropTypes.shape({
    id   : PropTypes.string,
    name : PropTypes.string,
  })),
};

export default InfoTable;
