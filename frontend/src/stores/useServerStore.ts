import { create } from 'zustand';
import { listServers } from '../services/api-client';

interface ServerState {
  servers: string[];
  currentServer: string;
  loading: boolean;
  error: string | null;
  fetchServers: () => Promise<void>;
  setCurrentServer: (server: string) => void;
}

export const useServerStore = create<ServerState>((set, get) => ({
  servers: [],
  currentServer: '',
  loading: false,
  error: null,

  fetchServers: async () => {
    set({ loading: true, error: null });
    try {
      const data = await listServers();
      const servers = data.servers;
      set({ servers, loading: false });

      // Set first server as default if not set
      const current = get().currentServer;
      if (!current && servers.length > 0) {
        set({ currentServer: servers[0] });
      }
    } catch (err) {
      set({ error: err instanceof Error ? err.message : 'Failed to fetch servers', loading: false });
    }
  },

  setCurrentServer: (server: string) => {
    set({ currentServer: server });
  },
}));
