// apps/web/src/App.jsx
import React from 'react';
import { BrowserRouter as Router, Routes, Route } from 'react-router-dom';
import Layout from './components/Layout';

// Placeholder Components
const Home = () => <div className="bg-white p-6 rounded-lg shadow">Welcome to the Analytics Toolkit. Select a view from the menu to begin.</div>;
const Placeholder = ({ title }) => <div className="bg-white p-6 rounded-lg shadow">{title} visualization coming soon...</div>;

function App() {
  return (
    <Router>
      <Layout>
        <Routes>
          <Route path="/" element={<Home />} />
          <Route path="/live" element={<Placeholder title="Live Game Stats" />} />
          <Route path="/defense" element={<Placeholder title="Defensive Trends" />} />
          <Route path="/players" element={<Placeholder title="Player Insights" />} />
          <Route path="/settings" element={<Placeholder title="Settings" />} />
        </Routes>
      </Layout>
    </Router>
  );
}

export default App;
