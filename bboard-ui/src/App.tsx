// This file is part of midnightntwrk/example-counter.
// Copyright (C) 2025 Midnight Foundation
// SPDX-License-Identifier: Apache-2.0
// Licensed under the Apache License, Version 2.0 (the "License");
// You may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import React, { useCallback, useEffect, useState } from 'react';
import { Box } from '@mui/material';
import { MainLayout, Board } from './components';
import { useDeployedBoardContext } from './hooks';
import { type BoardDeployment } from './contexts';
import { type Observable } from 'rxjs';
import css from './index.css'

interface ButtonProps {
  onClick: () => void;
  isActive: boolean;
  children: React.ReactNode;
}

export const styles = {
  container: {
    fontFamily: 'sans-serif',
    maxWidth: '600px',
    margin: 'auto',
    padding: '20px',
    border: '1px solid #ccc',
    borderRadius: '8px',
    backgroundColor: '#fff'
  },
  button: (isActive: boolean) => ({
    padding: '10px 20px',
    fontSize: '14px',
    backgroundColor: isActive ? '#007bff' : '#f8f9fa',
    color: isActive ? 'white' : '#333',
    border: '1px solid #dee2e6',
    borderRadius: '4px',
    cursor: 'pointer'
  }),
  uploadArea: {
    border: '2px dashed #dee2e6',
    borderRadius: '8px',
    padding: '20px',
    textAlign: 'center' as const,
    backgroundColor: '#f8f9fa'
  },
  resultContainer: {
    marginTop: '20px',
    padding: '20px',
    border: '1px solid #dee2e6',
    borderRadius: '8px',
    backgroundColor: '#f8f9fa',
    boxShadow: '0 2px 4px rgba(0,0,0,0.05)'
  },
  resultItem: {
    padding: '10px',
    backgroundColor: 'white',
    borderRadius: '4px',
    border: '1px solid #e9ecef'
  }
};


/**
 * The root bulletin board application component.
 *
 * @remarks
 * The {@link App} component requires a `<DeployedBoardProvider />` parent in order to retrieve
 * information about current bulletin board deployments.
 *
 * @internal
 */
const App: React.FC = () => {
  const boardApiProvider = useDeployedBoardContext();
  const [boardDeployments, setBoardDeployments] = useState<Array<Observable<BoardDeployment>>>([]);

  useEffect(() => {
    const subscription = boardApiProvider.boardDeployments$.subscribe(setBoardDeployments);

    return () => {
      subscription.unsubscribe();
    };
  }, [boardApiProvider]);


  const onJoinBoard = useCallback(
    (contractAddress: any) => boardApiProvider.resolve(contractAddress),
    [boardApiProvider],
  );

  // useEffect(() => {
  //    //SE DEFINI ADDRESS DEL CONTTR

  // }, [])


  const SwitchButton: React.FC<ButtonProps> = ({ onClick, isActive, children }) => (
    <button
      onClick={onClick}
      style={styles.button(isActive)}
    >
      {children}
    </button>
  );

  return (
    <Box sx={{ background: '#000', minHeight: '100vh' }}>
      <MainLayout>
      <center>
        
  <button
    className="connect-wallet-btn"
    onClick={() =>
      onJoinBoard("0200de94cf654bec52f403f235dd324f7613098bd862b4df3b9c22b6c3172de6709b")
    }
  >
    🚀 Conectar Wallet
  </button>

  

</center>

        {boardDeployments.map((boardDeployment, idx) => (
          <div data-testid={`board-${idx}`} key={`board-${idx}`}>
            <Board boardDeployment$={boardDeployment} />
          </div>
        ))}
        <div data-testid="board-start">
          {/* <Board /> */}
        </div>
      </MainLayout>
    </Box>
  );
};

export default App;