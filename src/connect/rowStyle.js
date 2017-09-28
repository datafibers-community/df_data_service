const rowStyle = (record) => {
    if (record.status === 'RUNNING') return { backgroundColor: '#dfd' };
    if (record.status === 'PAUSED' || record.status === 'UNASSIGNED') return { backgroundColor: '#ffff00' };
    if (record.status === 'FAILED') return { backgroundColor: '#fdd' };
    return {};
};

export default rowStyle;
