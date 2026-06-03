import { readFile } from "node:fs/promises";

export type User = {
  id: string;
  name: string;
};

export async function loadUser(path: string): Promise<User> {
  const raw = await readFile(path, "utf8");
  return JSON.parse(raw) as User;
}

export class UserPresenter {
  constructor(private readonly user: User) {}

  label(): string {
    return `${this.user.name} (${this.user.id})`;
  }
}

// footer comment
