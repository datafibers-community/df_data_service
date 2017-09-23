import { put, takeEvery } from 'redux-saga/effects';
import { push } from 'react-router-redux';
import { showNotification } from 'admin-on-rest';
import {
    REVIEW_APPROVE_SUCCESS,
    REVIEW_APPROVE_FAILURE,
    REVIEW_REJECT_SUCCESS,
    REVIEW_REJECT_FAILURE,
} from './reviewActions';

export default function* connectSaga() {
    yield [
        takeEvery(REVIEW_APPROVE_SUCCESS, function* () {
            yield put(showNotification('resources.reviews.notification.approved_success'));
            yield put(push('/ps'));
        }),
        takeEvery(REVIEW_APPROVE_FAILURE, function* ({ error }) {
            yield put(showNotification('resources.reviews.notification.approved_error', 'warning'));
            console.error(error);
        }),
        takeEvery(REVIEW_REJECT_SUCCESS, function* () {
            yield put(showNotification('resources.reviews.notification.rejected_success'));
            yield put(push('/ps'));
        }),
        takeEvery(REVIEW_REJECT_FAILURE, function* ({ error }) {
            yield put(showNotification('resources.reviews.notification.rejected_error', 'warning'));
            console.error(error);
        }),
    ];
}
