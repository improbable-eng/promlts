import React, { FC } from 'react';
import { Container } from 'reactstrap';
import styles from './NotFound.module.css';
import { usePathPrefix } from '../../../contexts/PathPrefixContext';

interface NotFoundProps {
  default: boolean;
  defaultRoute: string;
}

const NotFound: FC<NotFoundProps> = ({ defaultRoute }) => {
  const pathPrefix = usePathPrefix();
  return (
    <Container className={styles.container}>
      <h1>404, Page not Found!</h1>
      <h5>
        <a href={`${pathPrefix}${defaultRoute}`}>Go back to home page</a>
      </h5>
    </Container>
  );
};

export default NotFound;
