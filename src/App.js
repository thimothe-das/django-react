
import logo from './logo.svg';
import './App.css';
import React, { Component } from 'react';
import axios from 'axios';

class App extends Component {

  state = {
    title: '',
    content: '',
    file: null,
    isLoaded: false,
  };

  handleChange = (e) => {
    this.setState({
      [e.target.id]: e.target.value
    })
  };

  handleImageChange = (e) => {
    this.setState({
      file: e.target.files[0]
    })
  };



  handleSubmit = (e) => {
    e.preventDefault();
    console.log(this.state);
    let form_data = new FormData();
    form_data.append('file', this.state.file, this.state.file.name);
    let url = 'http://localhost:8000/api/posts/';
    axios.post(url, form_data, {
      headers: {
        'content-type': 'multipart/form-data'
      }
    })
        .then(res => {
          console.log(res.data.file);
        })
        .catch(err => console.log(err))
    this.setState({
      isLoaded: true
    })
  };

  render() {
  	const isLoaded = this.state.isLoaded;
  	let button;
  	if (isLoaded) {
  	      button = <div><button className="btn-download"><a href={"https://compartiment-thimothe.s3.eu-west-3.amazonaws.com/" + 'export' + this.state.file.name.replace(/ /g, "_")} download>Download</a></button></div>;
  	    } else {
  	      button = <div><p>File not loaded</p></div>;
  	    }
    return (
      <div className="App">
        <form onSubmit={this.handleSubmit}>

          <p>
            <input className="drop" type="file"
                   id="image"
                   accept=".csv, application/vnd.ms-excel"  onChange={this.handleImageChange} required/>
          </p>
          <input type="submit" value="Load"/>
         {button}
        </form>



      </div>
    );
  }
}

export default App;
