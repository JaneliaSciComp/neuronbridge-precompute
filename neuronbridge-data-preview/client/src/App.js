import React from 'react';
import { BrowserRouter as Router, Routes, Route, Link } from 'react-router-dom';
import './App.css';
import NeuronList from './components/NeuronList';
import NeuronDetail from './components/NeuronDetail';

function App() {
  return (
    <Router>
      <div className="App">
        <header className="App-header">
          <div className="container">
            <h1>NeuronBridge Data Preview</h1>
            <nav className="nav">
              <Link to="/">Home</Link>
            </nav>
          </div>
        </header>

        <main className="container">
          <Routes>
            <Route path="/" element={<NeuronList />} />
            <Route path="/neuron/:id" element={<NeuronDetail />} />
          </Routes>
        </main>

        <footer className="App-footer">
          <div className="container">
            <p>NeuronBridge Data Preview - Local data browser for neuron color depth MIP searches</p>
          </div>
        </footer>
      </div>
    </Router>
  );
}

export default App;
