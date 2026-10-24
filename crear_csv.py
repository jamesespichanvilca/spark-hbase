import csv
import random

nombres = [
    "Carlos", "Ana", "Luis", "Maria", "Jose", "Lucia",
    "Pedro", "Sofia", "Miguel", "Valentina", "Diego", "Camila",
    "Jorge", "Elena", "Andres", "Gabriela", "Daniel", "Paula",
    "Fernando", "Isabella", "Ricardo", "Martina", "Alberto", "Claudia"
]

apellidos = [
    "Garcia", "Rodriguez", "Lopez", "Martinez", "Perez",
    "Sanchez", "Ramirez", "Torres", "Flores", "Rivera",
    "Gomez", "Diaz", "Vargas", "Castro", "Rojas",
    "Mendoza", "Herrera", "Medina", "Silva", "Cruz"
]

ciudades = [
    "Lima", "Arequipa", "Cusco", "Trujillo", "Piura",
    "Chiclayo", "Iquitos", "Tacna", "Huancayo", "Puno",
    "Bogota", "Medellin", "Santiago", "Buenos Aires",
    "Quito", "Montevideo", "Madrid", "Barcelona"
]

cargos = [
    "Software Engineer", "Data Analyst", "Project Manager",
    "Human Resources Specialist", "Accountant",
    "Marketing Manager", "Sales Representative",
    "Business Analyst", "Product Manager", "Designer",
    "IT Support Specialist", "Operations Manager",
    "Financial Analyst", "Customer Service Representative",
    "Systems Administrator", "Data Scientist",
    "UX Designer", "DevOps Engineer", "Consultant",
    "Administrative Assistant"
]

with open("usuarios.csv", "w", newline="", encoding="utf-8-sig") as archivo:
    writer = csv.writer(archivo)

    writer.writerow(["id", "name", "city", "designation", "salary"])

    for user_id in range(1, 1001):
        nombre = random.choice(nombres)
        apellido = random.choice(apellidos)
        name = f"{nombre} {apellido}"
        city = random.choice(ciudades)
        designation = random.choice(cargos)

        # Salario aleatorio entre 1.500 y 15.000
        salary = random.randrange(1500, 15001, 100)

        writer.writerow([
            user_id,
            name,
            city,
            designation,
            salary
        ])

print("Archivo usuarios.csv creado con 1000 usuarios.")

