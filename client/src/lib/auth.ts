import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { apiRequest } from "./queryClient";
import { User } from "@shared/schema";

interface LoginCredentials {
  username: string;
  password: string;
}

interface AuthResponse {
  user: User;
  token: string;
}

// DEV MODE: Always treat user as logged in
export function useAuth() {
  // Dummy user object (customize as needed)
  const user = {
    id: "1",
    username: "admin",
    email: "admin@example.com",
    password: "",
    firstName: "Admin",
    lastName: "User",
    role: "admin",
    permissions: [],
    lastLogin: null,
    isActive: true,
    createdAt: null,
    updatedAt: null,
  } as User;
  return { user, isLoading: false };
}

export function useLogin() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: async (credentials: LoginCredentials): Promise<AuthResponse> => {
      const response = await apiRequest("POST", "/api/auth/login", credentials);
      return response.json();
    },
    onSuccess: (data) => {
      localStorage.setItem("auth_token", data.token);
      queryClient.setQueryData(["/api/auth/me"], data.user);
    },
  });
}

export function useLogout() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: async () => {
      await apiRequest("POST", "/api/auth/logout");
    },
    onSuccess: () => {
      localStorage.removeItem("auth_token");
      queryClient.setQueryData(["/api/auth/me"], null);
      queryClient.clear();
    },
  });
}
