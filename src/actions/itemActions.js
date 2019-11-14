import axios from 'axios';
import { GET_ITEMS, ADD_ITEM, DELETE_ITEM, ITEMS_LOADING } from './types';

export const getItems = () => dispatch => {
  dispatch(setItemsLoading());
  axios.get('http://localhost:8000/api/1').then(res =>
    dispatch({
        type: GET_ITEMS,
        payload: res.data
    })
  );
};

export const addItem = item => dispatch => {
  axios.post('http://localhost:8000/api/', item).then(res =>
    dispatch({
      type: ADD_ITEM,
      payload: res.data
    })
  );
};

export const deleteItem = id => dispatch => {
  axios.delete(`http://localhost:8000/api/${id}`).then(res =>
    dispatch({
      type: DELETE_ITEM,
      payload: id
    })
  );
};

export const setItemsLoading = () => {
  return {
    type: ITEMS_LOADING
  };
};

 {/* lie l'api pour pouvoir faire fonctionenr la suppression, ajout ou le display des items   */}