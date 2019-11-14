
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
    form_data.append('title', this.state.title);
    form_data.append('content', this.state.content);
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
  	      button = <div><a href={"https://compartiment-thimothe.s3.eu-west-3.amazonaws.com/" + 'export' + this.state.file.name.replace(/ /g, "_")} download>Click to download</a></div>;
  	    } else {
  	      button = <div><p>File not loaded</p></div>;
  	    }
    return (
      <div className="App">
        <form onSubmit={this.handleSubmit}>
          <p>
            <input type="text" placeholder='Title' id='title' value={this.state.title} onChange={this.handleChange} required/>
          </p>
          <p>
            <input type="text" placeholder='Content' id='content' value={this.state.content} onChange={this.handleChange} required/>

          </p>
          <p>
            <input type="file"
                   id="image"
                   accept=".csv, application/vnd.ms-excel"  onChange={this.handleImageChange} required/>
          </p>
          <input type="submit"/>
         {button}
        </form>



      </div>
    );
  }
}

export default App;
