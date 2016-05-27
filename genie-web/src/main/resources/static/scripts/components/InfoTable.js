import React from 'react';
import { render } from 'react-dom';
import { Link } from 'react-router'

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
          <td>{info.id}</td>
          <td><Link target="_blank" to={`${props.type}?name=${info.name}`}>{info.name}</Link></td>
        </tr>
      )}
    </tbody>
  </table>;

export default InfoTable;


