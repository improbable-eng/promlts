import React, { FC } from 'react';
import { RouteComponentProps } from '@reach/router';
import { UncontrolledAlert } from 'reactstrap';
import { withStatusIndicator } from '../../../components/withStatusIndicator';
import { useFetch } from '../../../hooks/useFetch';
import { Store } from './store';
import { StorePoolPanel } from './StorePoolPanel';
import { usePathPrefix } from '../../../contexts/PathPrefixContext';
import { API_PATH } from '../../../constants/constants';

export interface StoreListProps {
  [storeType: string]: Store[];
}

export const StoreContent: FC<{ data: StoreListProps }> = ({ data }) => {
  const storePools = Object.keys(data);
  return (
    <>
      {storePools.length > 0 ? (
        storePools.map<JSX.Element>(storeGroup => (
          <StorePoolPanel key={storeGroup} title={storeGroup} storePool={data[storeGroup]} />
        ))
      ) : (
        <UncontrolledAlert color="warning">No stores registered.</UncontrolledAlert>
      )}
    </>
  );
};

const StoresWithStatusIndicator = withStatusIndicator(StoreContent);

export const Stores: FC<RouteComponentProps> = () => {
  const pathPrefix = usePathPrefix();
  const { response, error, isLoading } = useFetch<StoreListProps>(`${pathPrefix}/${API_PATH}/stores`);
  const { status: responseStatus } = response;
  const badResponse = responseStatus !== 'success' && responseStatus !== 'start fetching';

  return (
    <StoresWithStatusIndicator
      data={response.data}
      error={badResponse ? new Error(responseStatus) : error}
      isLoading={isLoading}
    />
  );
};

export default Stores;
