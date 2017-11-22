import React, { Component } from 'react'
import FlatButton from 'material-ui/FlatButton'
import { CreateButton }from 'admin-on-rest';
import { CardActions } from 'material-ui/Card'
import NavigationRefresh from 'material-ui/svg-icons/navigation/refresh'
import { connect } from 'react-redux'
import { refreshView as refreshViewAction } from 'admin-on-rest'

class MyRefresh extends Component {
    componentDidMount() {
        const { refreshInterval, refreshView } = this.props
        if (refreshInterval) {
            this.interval = setInterval(() => {
                refreshView()
            }, refreshInterval)
        }
    }

    componentWillUnmount() {
        clearInterval(this.interval)
    }

    render() {
        const { label, refreshView, icon } = this.props;
        return (
            <FlatButton
                primary
                label={label}
                onClick={refreshView}
                icon={icon}
            />
        );
    }
}

const RefreshButton = connect(null, { refreshView: refreshViewAction })(MyRefresh)

const RefreshListActions = ({ resource, filters, displayedFilters, filterValues, hasCreate, basePath, showFilter, refreshInterval }) => (
    <CardActions>
        {filters && React.cloneElement(filters, { resource, showFilter, displayedFilters, filterValues, context: 'button' }) }
        {hasCreate && <CreateButton basePath={basePath} />}
        <RefreshButton primary label="Refresh" refreshInterval={refreshInterval} icon={<NavigationRefresh />} />
    </CardActions>
);

export default RefreshListActions