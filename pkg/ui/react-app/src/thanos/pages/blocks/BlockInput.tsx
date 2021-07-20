import React, { FC, ChangeEvent } from 'react';
import { Button, InputGroup, InputGroupAddon, InputGroupText, Input } from 'reactstrap';
import { FontAwesomeIcon } from '@fortawesome/react-fontawesome';
import { faSearch } from '@fortawesome/free-solid-svg-icons';
import styles from './blocks.module.css';

interface BlockInputProps {
  onChange: ({ target }: ChangeEvent<HTMLInputElement>) => void;
  onClick: () => void;
}

export const BlockInput: FC<BlockInputProps> = ({ onChange, onClick }) => {
  return (
    <>
      <InputGroup className={styles.blockInput}>
        <InputGroupAddon addonType="prepend">
          <InputGroupText>
            <FontAwesomeIcon icon={faSearch} />
          </InputGroupText>
        </InputGroupAddon>
        <Input placeholder="Search block by ulid" onChange={onChange} />
        <InputGroupAddon addonType="append">
          <Button className="execute-btn" color="primary" onClick={onClick}>
            Search
          </Button>
        </InputGroupAddon>
      </InputGroup>
    </>
  );
};
