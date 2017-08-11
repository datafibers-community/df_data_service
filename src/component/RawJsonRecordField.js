import React from 'react';
import PropTypes from 'prop-types';
import { Link } from 'react-router-dom';
import IconButton from 'material-ui/IconButton';
import {cyan500} from 'material-ui/styles/colors';
import ContentCreate from 'material-ui/svg-icons/content/create';

const RawJsonRecordField = ({ record, source }) => <pre dangerouslySetInnerHTML={{ __html: JSON.stringify(record, null, '\t')}}></pre>;
RawRecordField.defaultProps = { label: 'Raw Json' };

EditButton.propTypes = {
    basePath: PropTypes.string,
    record: PropTypes.object,
};

RawJsonRecordField.defaultProps = {
    label: 'Raw Json',
    style: { padding: 0 },
};

export default RawJsonRecordField;
