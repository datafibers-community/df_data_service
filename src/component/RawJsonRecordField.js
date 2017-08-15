import React from 'react';
import PropTypes from 'prop-types';

const RawJsonRecordField = ({ record, source }) => <pre dangerouslySetInnerHTML={{ __html: JSON.stringify(record, null, '\t')}}></pre>;

RawJsonRecordField.propTypes = {
    record: PropTypes.object,
    source: PropTypes.string,
};

RawJsonRecordField.defaultProps = {
    label: 'Raw Json',
    style: { padding: 0 },
};

export default RawJsonRecordField;
