import Page from './Page';
import TableRow from './components/JobTableRow';
import JobDetails from './components/JobDetails';

export default class Job extends Page {

  get url() {
    return '/api/v3/jobs';
  }

  get dataKey() {
    return 'jobSearchResultList';
  }

  get formFields() {
    return [
      {
        label : 'User Name',
        name  : 'user',
        value : '',
        type  : 'input',
      }, {
        label : 'Job Id',
        name  : 'id',
        value : '',
        type  : 'input',
      }, {
        label : 'Job Name',
        name  : 'name',
        value : '',
        type  : 'input',
      }, {
        label : 'Status',
        name  : 'status',
        value : '',
        type  : 'option',
        optionValues: ['', 'INIT', 'RUNNING', 'SUCCEEDED', 'FAILED', 'KILLED', 'INVALID'],
      }, {
        label : 'Size',
        name  : 'size',
        value : 25,
        type  : 'option',
        optionValues: [10, 25, 50, 100],
      }, {
        label : 'Sort By',
        name  : 'sort',
        value : '',
        type  : 'select',
        selectFields: ['user', 'id', 'name', 'status', 'clusterName', 'cluserId', 'started', 'finished'].map(field => (
          {
            value: field,
            label: field,
          }
        )),
      },
    ];
  }

  get hiddenFormFields() {
    return [
      {
        label : 'Tags',
        name  : 'tag',
        value : '',
        type  : 'input',
      }, {
        label : 'Cluster Name',
        name  : 'clusterName',
        value : '',
        type  : 'input',
      }, {
        label : 'Cluster ID',
        name  : 'cluserId',
        value : '',
        type  : 'input',
      }, {
        label : 'Command Name',
        name  : 'commandName',
        value : '',
        type  : 'input',
      }, {
        label : 'Command ID',
        name  : 'commandId',
        value : '',
        type  : 'input',
      },
    ];
  }

  get searchPath() {
    return 'jobs';
  }

  get rowType() {
    return TableRow;
  }

  get tableHeader() {
    return (
        ['Id', 'Name', 'Output', 'Copy Link', 'User', 'Status', 'Cluster', 'Started', 'Finished', 'Run Time']
    );
  }

  get detailsTable() {
    return JobDetails;
  }
}
