from cryptography.fernet import Fernet

# Generate a key and save it securely (do this once and reuse the key)

# Load the key from the file
def load_key():
    with open("encryption_key.key", "rb") as key_file:
        return key_file.read()

encrypted_password = "gAAAAABnkgGhuY7xRm0SsFZGFyb2MIr4NrEZuvuOfc7CpFQNI_VEh8M9IcVlWP8133O4JiQyEw55O7sE8G9bH70f2Id4JBlWqw=="
key = load_key()

def generate_key():
    key = Fernet.generate_key()
    with open("encryption_key.key", "wb") as key_file:
        key_file.write(key)
    print("Key generated and saved to 'encryption_key.key'.")



# Encrypt the password
def encrypt_password(password, key):
    fernet = Fernet(key)
    encrypted_password = fernet.encrypt(password.encode())
    return encrypted_password

# Decrypt the password
def decrypt_password(encrypted_password, key):
    fernet = Fernet(key)
    decrypted_password = fernet.decrypt(encrypted_password).decode()
    return decrypted_password

# Example Usage
if __name__ == "__main__":
    # Generate and save a key (run this once and comment it out afterward)
    #generate_key()

    # Load the key
    key = load_key()

    # Password to encrypt
    original_password = "abc"
    # Encrypt the password
    # encrypted_password = encrypt_password(original_password, key)
    # print("Encrypted Password:", encrypted_password)
    # Decrypt the password
    decrypted_password = decrypt_password("gAAAAABnkgGhuY7xRm0SsFZGFyb2MIr4NrEZuvuOfc7CpFQNI_VEh8M9IcVlWP8133O4JiQyEw55O7sE8G9bH70f2Id4JBlWqw==", key)
    print("Decrypted Password:", decrypted_password)

