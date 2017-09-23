import React from 'react';
import { CardActions } from 'material-ui/Card';
import { ListButton, DeleteButton, RefreshButton } from 'admin-on-rest';
import AcceptButton from './AcceptButton';
import RejectButton from './RejectButton';
import RestartButton from './RestartButton';

const cardActionStyle = {
    zIndex: 2,
    display: 'inline-block',
    float: 'right',
};

const ReviewEditActions = ({ basePath, data, hasDelete, hasShow, refresh }) => (
    <CardActions style={cardActionStyle}>
        <ListButton basePath={basePath} />
        <AcceptButton record={data} />
        <RejectButton record={data} />
        <RestartButton record={data} />
        <DeleteButton basePath={basePath} record={data} />
        <RefreshButton refresh={refresh} />
    </CardActions>
);

export default ReviewEditActions;
