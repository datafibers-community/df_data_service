import React from 'react';
import PropTypes from 'prop-types';
import get from 'lodash.get';

const RawJsonRecordSpecificField = ({ record, source }) => <pre dangerouslySetInnerHTML={{ __html: JSON.stringify(get(record, source), null, '\t')}}></pre>;

RawJsonRecordSpecificField.propTypes = {
    record: PropTypes.object,
    source: PropTypes.string,
};

RawJsonRecordSpecificField.defaultProps = {
    label: 'Raw Json',
    style: { padding: 0 },
};

export default RawJsonRecordSpecificField;
