import { RouteComponentProps } from '@reach/router';
import React, { FC } from 'react';
import { useFetch } from '../../../hooks/useFetch';
import PathPrefixProps from '../../../types/PathPrefixProps';
import { Table } from 'reactstrap';
import { withStatusIndicator } from '../../../components/withStatusIndicator';

interface ConfigFilesMap {
  [key: string]: string;
}

interface ConfigFileProps {
  data?: ConfigFilesMap;
}

const ConfigFilesContent: FC<ConfigFileProps> = ({ data = {} }) => {
  const keys = Object.keys(data);
  return keys.length > 0 ? (
    <>
      <h2>Configuration Files</h2>
      <Table bordered striped>
        <tbody>
          {keys.map(key => (
            <tr key={key}>
              <th>{key}</th>
              <td className="config-file-cell">{data[key]}</td>
            </tr>
          ))}
        </tbody>
      </Table>
    </>
  ) : (
    <h2> No Configuration files found </h2>
  );
};
const ConfigFilesWithStatusIndicator = withStatusIndicator(ConfigFilesContent);

const ConfigFiles: FC<RouteComponentProps & PathPrefixProps> = ({ pathPrefix = '' }) => {
  const { response, error, isLoading } = useFetch<ConfigFilesMap>(`${pathPrefix}/api/v1/status/configfiles`);
  return <ConfigFilesWithStatusIndicator data={response.data} error={error} isLoading={isLoading} />;
};

export default ConfigFiles;
