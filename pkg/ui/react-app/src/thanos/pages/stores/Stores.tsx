import React, { FC } from 'react';
import { RouteComponentProps } from '@reach/router';
import { withStatusIndicator } from '../../../components/withStatusIndicator';
import { useFetch } from '../../../hooks/useFetch';
import { Store } from './store';
import { StorePoolPanel } from './StorePoolPanel';

interface StoreListProps {
  [storeType: string]: Store[];
}

export const StoreContent: FC<{ data: StoreListProps }> = ({ data }) => {
  return (
    <>
      {Object.keys(data).map<JSX.Element>(storeGroup => (
        <StorePoolPanel key={storeGroup} title={storeGroup} storePool={data[storeGroup]} />
      ))}
    </>
  );
};

const StoresWithStatusIndicator = withStatusIndicator(StoreContent);

export const Stores: FC<RouteComponentProps> = () => {
  const { response, error, isLoading } = useFetch<StoreListProps>(`/api/v1/stores`);
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
